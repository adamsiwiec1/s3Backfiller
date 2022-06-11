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
	"sync"
)

var (
	sess, _ = session.NewSession(&aws.Config{
		Region:      aws.String("us-east-1"),
		Credentials: credentials.NewSharedCredentials("", "session")})
	uploader   = s3manager.NewUploader(sess)
	downloader = s3manager.NewDownloader(sess)
	wg         sync.WaitGroup
)

func downloadParquet(bucket string, item string) {

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

func uploadParquet(bucket string, itemName string, itemPath string) string {
	file, _ := ioutil.ReadFile(itemPath)
	output, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(fmt.Sprintf("json/%s.json", itemName)),
		Body:   bytes.NewReader(file),
	})
	if err != nil {
		log.Fatalf("Unable to upload item %q, %v", itemPath, err)
	}
	fmt.Println("Uploaded", output.Location)
	return output.Location
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
	fmt.Println("Conversion complete", pqFilePath)
	return jsonFileName, jsonFilePath
}

func pullAndConvertBatch(srcBucket string, dstBucket string, batch []string) { //, ch chan<- string) {
	defer wg.Done()
	for i := 0; i < len(batch); i++ {
		downloadParquet(srcBucket, batch[i])
		var jsonLocalFileName, jsonLocalFilePath = convertToJsonLocal(batch[i])
		uploadParquet(dstBucket, jsonLocalFileName, jsonLocalFilePath)
	}
}

func processBatches(srcBucket string, dstBucket string, fileList []string, batchSize int) []string {
	var results []string
	var j int
	for i := 0; i < len(fileList); i += batchSize {
		j += batchSize
		if j > len(fileList) {
			j = len(fileList)
		}
		wg.Add(1)
		go pullAndConvertBatch(srcBucket, dstBucket, fileList[i:j]) //, ch)
		fmt.Println(fileList[i:j])
	}
	fmt.Println("waiting..")
	wg.Wait()
	fmt.Println("finished.")
	return results
}

func main() {
	fileList := []string{"userdata1.parquet",
		"userdata2.parquet",
		"userdata3.parquet",
		"userdata4.parquet",
		"userdata5.parquet",
		"userdata6.parquet",
		"userdata7.parquet",
		"userdata8.parquet",
		"userdata9.parquet",
		"userdata10.parquet",
		"userdata11.parquet",
		"userdata12.parquet",
		"userdata13.parquet",
		"userdata14.parquet"}
	defer processBatches("s3-backfiller-src", "s3-backfiller-dst", fileList, 4)
}
