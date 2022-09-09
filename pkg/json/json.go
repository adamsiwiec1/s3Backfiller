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
	// Format file name
	re := regexp.MustCompile("([^.]*)")
	fileName := strings.Split(re.FindString(jsonFilePath), "/")[2]
	pqFileName = fmt.Sprintf("%s.parquet", fileName)

	// Step 1: Create Parquet Schema From Json
	var data []map[string]interface{}
	fileBs, err := os.ReadFile(jsonFilePath)
	if err != nil {
		fmt.Println(err)
	}
	err = json.Unmarshal(fileBs, &data)
	if err != nil {
		fmt.Println(err)
	}
	var fields []string
	var lastField int
	for k, v := range data[0] {
		lastField += 1
		t := fmt.Sprintf("%s", reflect.TypeOf(v))
		field := ""
		if strings.Contains(t, "nil") {
			field = fmt.Sprintf(`{"Tag":"name=%s, type=%s"}`, k, "BYTE_ARRAY")
		} else {
			field = fmt.Sprintf(`{"Tag":"name=%s, type=%s"}`, k, "BYTE_ARRAY") //goTypeToPqType(t))
		}
		if lastField != len(data[0]) { // lib Im using does not like trailing comma on last JSON element. removing.
			fields = append(fields, field+",")
		} else {
			fields = append(fields, field)
		}

	}
	// Step 1b**: Implement support for nested fields.

	// Step 2: Create Parquet File with the Defined Schema
	jsonSchema := fmt.Sprintf(`{"Tag": "%s","Fields": %s}`,
		fmt.Sprintf("name=%s, repetitiontype=%s", re.FindString(pqFileName), "OPTIONAL"), fmt.Sprintf("%s", fields))
	fmt.Println(jsonSchema)
	fw, err := local.NewLocalFileWriter("tmp/dst/" + pqFileName)
	if err != nil {
		log.Println("Can't create file", err)
		return
	}
	pw, err := writer.NewParquetWriter(fw, jsonSchema, 1)
	if err != nil {
		log.Println("Can't create json writer", err)
		return
	}

	// Step 3: Write each JSON Object Row by Row to the Parquet File
	pw.RowGroupSize = 128 * 1024 * 1024 //128M
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	for item := range data[:10] {
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
		fmt.Println(itemDict)
		if err = pw.Write(itemDict); err != nil {
			log.Println("Write error", err)
		}
	}

	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
		return
	}
	log.Println("Write Finished")
	fw.Close()

	// Step 4: Assert JSON Object Count to Parquet Row Count
	fr, err := local.NewLocalFileReader("tmp/dst/" + pqFileName)
	if err != nil {
		log.Println("Can't open file", err)
		return
	}
	pr, err := reader.NewParquetReader(fr, jsonSchema, 1)

	fmt.Println(pr.GetNumRows())
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}

	pr.ReadStop()
	fr.Close()
	return "need 2 implement", "need 2 implement"
}
