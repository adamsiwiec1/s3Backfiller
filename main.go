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
	"regexp"
)

// to do:
// - mkdirs on startup if they are not made already

func downloadParquet(bucket string, item string) {
	// session
	sess, _ := session.NewSession(&aws.Config{
		Region:      aws.String("us-east-1"),
		Credentials: credentials.NewSharedCredentials("", "session"),
	})

	// downloader
	downloader := s3manager.NewDownloader(sess)

	// act
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

func convertToJsonLocal(pqFilePath string) {
	var jsonFileName = pqFilePath
	pqFilePath = fmt.Sprintf("tmp/pq/%s", pqFilePath)
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
	re := regexp.MustCompile("([^.]*)")
	jsonFileName = re.FindString(jsonFileName)
	_ = ioutil.WriteFile(fmt.Sprintf("tmp/json/%s.json", jsonFileName), jsonBs, 0644)
	//_ = ioutil.WriteFile(fmt.Sprintf("%s.json", re.FindString(pqFilePath)), jsonBs, 0644)
}

func pullAndConvertBatch(bucket string, files []string) {
	for i := 0; i < len(files); i++ {
		downloadParquet(bucket, files[i])
		convertToJsonLocal(files[i])
	}
}

func main() {
	fileList := []string{"userdata1.parquet",
		"userdata2.parquet",
		"userdata3.parquet",
		"userdata4.parquet",
		"userdata5.parquet"}
	pullAndConvertBatch("s3-backfiller-src", fileList)
}
