package pq

import (
	"encoding/json"
	"fmt"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"io/ioutil"
	"log"
	"regexp"
)

func ConvertPqToJsonLocal(pqFilePath string) (string, string) {

	var jsonFileName = pqFilePath
	pqFilePath = fmt.Sprintf("tmp/src/%s", pqFilePath)
	fr, err := local.NewLocalFileReader(pqFilePath)
	if err != nil {
		log.Println("Can't open file.", err)
	}

	pr, err := reader.NewParquetReader(fr, nil, 4)
	if err != nil {
		log.Println("Can't create parquet reader.", err)
	}
	num := 0
	num = int(pr.GetNumRows())
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
	jsonFilePath := fmt.Sprintf("tmp/dst/%s.json", jsonFileName)
	_ = ioutil.WriteFile(jsonFilePath, jsonBs, 0644)
	fmt.Println("conversion complete..", pqFilePath)
	return jsonFileName, jsonFilePath
}
