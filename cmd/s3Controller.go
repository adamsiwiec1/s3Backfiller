package cmd

import (
	"bytes"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"io/ioutil"
	"log"
	"os"
	"sync"
)

var (
	maxRetries  = 3
	maxPartSize = int64(200 * 1024 * 1024) // 1024 *1024 = ab 1 mb
	sess, _     = session.NewSession(&aws.Config{
		Region:      aws.String("us-east-1"),
		Credentials: credentials.NewSharedCredentials("", "session")})
	uploader   = s3manager.NewUploader(sess)
	downloader = s3manager.NewDownloader(sess)
	svc        = s3.New(sess)
	//svc        = s3Controller.New(sess) // this obj gives you a larger amt of s3Controller actions compared to s3manager
	wg sync.WaitGroup
)

func crawlBucket(srcBucket string) []string { // need to recursively crawl idk if this does
	var items []string
	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(srcBucket)})
	if err != nil {
		fmt.Println("Error crawling bucket..")
	}
	for _, item := range resp.Contents {
		fmt.Println(*item.Key)
		items = append(items, *item.Key)
	}
	return items
}

func downloadObj(bucket string, item string) {
	file, err := os.Create(fmt.Sprintf("tmp/src/%s", item))
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

func pullAndConvertBatch(srcBucket string, dstBucket string, batch []string) {
	defer wg.Done()
	for i := 0; i < len(batch); i++ {
		downloadObj(srcBucket, batch[i])
		fileName, filePath := convertToJsonLocal(batch[i])
		regularUpload(dstBucket, fileName, filePath)
	}
}

func regularUpload(bucket string, itemName string, itemPath string) string {
	file, _ := ioutil.ReadFile(itemPath)
	output, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(fmt.Sprintf("jsonController/%s.jsonController", itemName)),
		Body:   bytes.NewReader(file),
	})
	if err != nil {
		log.Fatalf("Unable to upload item %q, %v", itemPath, err)
	}
	fmt.Println("Uploaded", output.Location)
	return output.Location
}

func processBatches(srcBucket string, dstBucket string, fileList []string, batchSize int) {
	var j int
	for i := 0; i < len(fileList); i += batchSize {
		j += batchSize
		if j > len(fileList) {
			j = len(fileList)
		}
		wg.Add(1)
		go pullAndConvertBatch(srcBucket, dstBucket, fileList[i:j])
		fmt.Println(fileList[i:j])
	}
	fmt.Println("waiting..")
	wg.Wait()
	fmt.Println("finished.")
}
