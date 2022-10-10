package json

import (
	"encoding/json"
	"fmt"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
	"log"
	"os"
	"reflect"
	"regexp"
	"strings"
)

func goTypeToPqType(goType string) string { // currently mixing primitive and logical types
	switch goType {
	case "string":
		return "BYTE_ARRAY"
	case "bool":
		return "BOOLEAN"
	case "int32":
		return "INT_32" // make this more robust to cover INT_8 and more granular pq logical types
	case "int64":
		return "INT64"
	case "float64":
		return "DOUBLE"
	case "slice":
		return "LIST"
	case "map":
		return "MAP"
	}
	return "BYTE_ARRAY" // make this not shitty
}

func ConvertJsonToPq(jsonFilePath string) (pqFileName string, pqFilePath string) {
	// prep
	re := regexp.MustCompile("([^.]*)")
	pqFileName = fmt.Sprintf("%s.parquet", re.FindString(jsonFilePath))
	pqFilePath = fmt.Sprintf("tmp/dst/{%s}", pqFileName)
	jsonFilePath = fmt.Sprintf("tmp/src/%s", jsonFilePath)
	var (
		data       []map[string]interface{}
		fields     []string
		lastField  int
		jsonSchema string
	)

	// Step 1: Create Parquet Schema From Json **need2 implement support for nested fields.

	fileBs, err := os.ReadFile(jsonFilePath)
	if err != nil {
		log.Printf("Error reading JSON file. %s:%s\n", jsonFilePath, err)
	}
	err = json.Unmarshal(fileBs, &data)
	if err != nil {
		log.Printf("Error unmarshalling JSON. %s:%s\n", jsonFilePath, err)
	}
	for k, v := range data[0] { // go through every field instead of the first to be certain
		lastField += 1
		t := fmt.Sprintf("%s", reflect.TypeOf(v))
		field := ""
		if strings.Contains(t, "nil") {
			field = fmt.Sprintf(`{"Tag":"name=%s, type=%s"}`, k, "BYTE_ARRAY")
		} else {
			field = fmt.Sprintf(`{"Tag":"name=%s, type=%s"}`, k, "BYTE_ARRAY") //goTypeToPqType(t))
		}
		if lastField != len(data[0]) { // lib does not like trailing comma on last JSON element. removing.
			fields = append(fields, field+",")
		} else {
			fields = append(fields, field)
		}
	}

	// Step 2: Create Parquet File with the Defined Schema

	jsonSchema = fmt.Sprintf(`{"Tag": "%s","Fields": %s}`,
		fmt.Sprintf("name=%s, repetitiontype=%s", re.FindString(pqFileName), "OPTIONAL"),
		fmt.Sprintf("%s", fields))
	fw, err := local.NewLocalFileWriter(pqFilePath)
	if err != nil {
		log.Printf("Cant create file. %s:%s\n", jsonFilePath, err)
		return
	}
	pw, err := writer.NewParquetWriter(fw, jsonSchema, 4)
	if err != nil {
		log.Printf("Cant create json writer. %s:%s\n", pqFilePath, err)
		return
	}

	// Step 3: Write each JSON Object Row by Row to the Parquet File

	pw.RowGroupSize = 128 * 1024 * 1024 //128M
	pw.CompressionType = parquet.CompressionCodec_SNAPPY
	for item := range data[0:1000] {
		itemDict := make(map[string]string)
		// handle interface datatypes and nulls
		for k, v := range data[item] {
			x := fmt.Sprintf("%v", v)
			if strings.Contains(x, "nil") {
				itemDict[k] = "NULL"
			} else {
				itemDict[k] = x
			}
		}
		if err = pw.Write(itemDict); err != nil {
			log.Printf("Write error. %s:%s\n", pqFilePath, err)
		}
	}
	if err = pw.WriteStop(); err != nil {
		log.Printf("WriteStop error. %s:%s\n", pqFilePath, err)
		return
	}
	log.Println("write finished..", pqFilePath)
	fw.Close()

	// Step 4: Assert JSON Object Count to Parquet Row Count

	fr, err := local.NewLocalFileReader(pqFilePath)
	if err != nil {
		log.Println("Can't open file", err)
		return
	}

	pr, err := reader.NewParquetReader(fr, jsonSchema, 1)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}

	pr.ReadStop()
	fr.Close()
	return pqFileName, pqFilePath
}

// test
