package s3

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
	"s3Backfiller/pkg/pq"
	"sync"
)

var (
	sess, _ = session.NewSession(&aws.Config{
		Region:      aws.String("us-east-1"),
		Credentials: credentials.NewSharedCredentials("", "session")})
	uploader   = s3manager.NewUploader(sess)
	downloader = s3manager.NewDownloader(sess)
	svc        = s3.New(sess)
	//svc        = s3Controller.New(sess) // this obj gives you a larger amt of s3Controller actions compared to s3manager
	wg sync.WaitGroup
)

func downloadObj(bucket string, item string) {
	file, err := os.Create(fmt.Sprintf("tmp/src/%s", item))
	if err != nil {
		log.Fatalf("Unable to create file. %q, %v", item, err)
	}

	numBytes, err := downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(item),
		})
	if err != nil {
		log.Fatalf("Unable to download item. %q, %v", item, err)
	}
	fmt.Println("downloaded..", file.Name(), numBytes, "bytes")
}

func pullAndConvertBatch(srcBucket string, dstBucket string, batch []string) {
	defer wg.Done()
	for i := 0; i < len(batch); i++ {
		downloadObj(srcBucket, batch[i])
		fileName, filePath := pq.ConvertPqToJsonLocal(batch[i])
		regularUpload(dstBucket, fileName, filePath)
	}
}

func regularUpload(bucket string, itemName string, itemPath string) string {
	file, _ := ioutil.ReadFile(itemPath)
	output, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(fmt.Sprintf("%s.json", itemName)),
		Body:   bytes.NewReader(file),
	})
	if err != nil {
		log.Fatalf("Unable to upload item %q, %v", itemPath, err)
	}
	fmt.Println("uploaded..", output.Location)
	return output.Location
}

func CrawlBucket(srcBucket string) []string { // need to recursively crawl IDK if this does
	var items []string
	fmt.Printf("crawling %s..\n", srcBucket)
	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(srcBucket)})
	if err != nil {
		fmt.Printf("error crawling bucket..%s\n", err)
	}
	for _, item := range resp.Contents {
		fmt.Println(*item.Key)
		items = append(items, *item.Key)
	}
	return items
}

func ProcessBatches(srcBucket string, dstBucket string, fileList []string, batchSize int) {
	var j int
	for i := 0; i < len(fileList); i += batchSize {
		j += batchSize
		if j > len(fileList) { // create the index vars for fileList
			j = len(fileList)
		}
		wg.Add(1)
		go pullAndConvertBatch(srcBucket, dstBucket, fileList[i:j]) // begin thread
		fmt.Println(fileList[i:j])
	}
	fmt.Println("waiting..")
	wg.Wait()
	fmt.Println("finished.")
}
