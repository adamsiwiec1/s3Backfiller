package main

import (
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"io/ioutil"
	"log"
	"os"
)

func downloadParquet(bucket string, item string) {
	// session
	sess, _ := session.NewSession(&aws.Config{
		Region:      aws.String("us-east-1"),
		Credentials: credentials.NewSharedCredentials("", "session"),
	})

	// downloader
	downloader := s3manager.NewDownloader(sess)

	// act
	file, err := os.Create(item)
	numBytes, err := downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(item),
		})
	if err != nil {
		log.Fatalf("Unable to download item %q, %v", item, err)
	}
	fmt.Println("Downloaded", file.Name(), numBytes, "bytes")
}

func convertToJsonFromLocal(pqFilePath string) {
	fr, err := local.NewLocalFileReader(pqFilePath)
	if err != nil {
		log.Println("Can't open file", err)
		return
	}

	pr, err := reader.NewParquetReader(fr, nil, 4)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}
	var num = 0
	num = int(pr.GetNumRows())
	res, err := pr.ReadByNumber(num)
	if err != nil {
		log.Println("Can't read", err)
		return
	}
	fmt.Println(res)
	jsonBs, err := json.Marshal(res)
	if err != nil {
		log.Println("Can't to json", err)
		return
	}
	fmt.Println(string(jsonBs))

	_ = ioutil.WriteFile("test.json", jsonBs, 0644)
}

func main() {
	downloadParquet("s3-backfiller-src", "userdata1.parquet")
	convertToJsonFromLocal("userdata1.parquet")
}
