package main

import (
	"bytes"
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
	"regexp"
)

// to do:
// - mkdirs on startup if they are not made already

var sess, _ = session.NewSession(&aws.Config{
	Region:      aws.String("us-east-1"),
	Credentials: credentials.NewSharedCredentials("", "session"),
})

func downloadParquet(bucket string, item string) {
	downloader := s3manager.NewDownloader(sess)
	file, err := os.Create(fmt.Sprintf("tmp/pq/%s", item))
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

func uploadParquet(bucket string, itemName string, itemPath string) {
	file, _ := ioutil.ReadFile(itemPath)
	uploader := s3manager.NewUploader(sess)
	log.Println(file)
	uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(fmt.Sprintf("tmp/json/%s.json", itemName)),
		Body:   bytes.NewReader(file),
	})
}

func convertToJsonLocal(pqFilePath string) (string, string) {
	var jsonFileName = pqFilePath
	pqFilePath = fmt.Sprintf("tmp/pq/%s", pqFilePath)
	fr, err := local.NewLocalFileReader(pqFilePath)
	if err != nil {
		log.Println("Can't open file", err)
	}

	pr, err := reader.NewParquetReader(fr, nil, 4)
	if err != nil {
		log.Println("Can't create parquet reader", err)
	}
	var num = 0
	num = int(pr.GetNumRows())
	res, err := pr.ReadByNumber(num)
	if err != nil {
		log.Println("Can't read", err)
	}
	jsonBs, err := json.Marshal(res)
	if err != nil {
		log.Println("Can't to json", err)
	}
	re := regexp.MustCompile("([^.]*)")
	jsonFileName = re.FindString(jsonFileName)
	var jsonFilePath = fmt.Sprintf("tmp/json/%s.json", jsonFileName)
	_ = ioutil.WriteFile(jsonFilePath, jsonBs, 0644)
	return jsonFileName, jsonFilePath
}

func pullAndConvertBatch(srcBucket string, dstBucket string, files []string) {
	for i := 0; i < len(files); i++ {
		downloadParquet(srcBucket, files[i])
		var jsonLocalFileName, jsonLocalFilePath = convertToJsonLocal(files[i])
		uploadParquet(dstBucket, jsonLocalFileName, jsonLocalFilePath)
	}
}

func main() {
	fileList := []string{"userdata1.parquet",
		"userdata2.parquet",
		"userdata3.parquet",
		"userdata4.parquet",
		"userdata5.parquet"}
	pullAndConvertBatch("s3-backfiller-src", "s3-backfiller-dst", fileList)
}
