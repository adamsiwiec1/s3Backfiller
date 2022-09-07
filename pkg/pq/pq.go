package pq

import (
	"encoding/json"
	"fmt"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"io/ioutil"
	"log"
	"os"
	"regexp"
)

func ConvertPqToJson(pqFilePath string) (jsonFileName string, jsonFilePath string) {
	jsonFileName = pqFilePath
	pqFilePath = fmt.Sprintf("tmp/src/%s", pqFilePath)
	fr, err := local.NewLocalFileReader(pqFilePath)
	if err != nil {
		log.Println("Failed to open file.", err)
	}
	pr, err := reader.NewParquetReader(fr, nil, 4)
	if err != nil {
		log.Println("Failed to create parquet reader.", err)
	}
	num := int(pr.GetNumRows())
	res, err := pr.ReadByNumber(num)
	if err != nil {
		log.Println("Can't read.", err)
	}
	jsonBs, err := json.Marshal(res)
	if err != nil {
		log.Println("Can't to json.", err)
	}
	re := regexp.MustCompile("([^.]*)")
	jsonFileName = re.FindString(jsonFileName)
	jsonFilePath = fmt.Sprintf("tmp/dst/%s.json", jsonFileName)
	_ = ioutil.WriteFile(jsonFilePath, jsonBs, 0644)
	log.Println("conversion complete..", pqFilePath)
	err = os.Remove(pqFilePath) // remove file after conversion
	if err != nil {
		log.Printf("Error removing file after conversion. %s", pqFilePath)
	}
	return jsonFileName, jsonFilePath
}
