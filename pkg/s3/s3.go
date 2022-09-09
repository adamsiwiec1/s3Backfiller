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
	"s3Backfiller/pkg/json"
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

func conversionSwitch(srcType string, dstType string, item string) (fileName string, filePath string) {
	// item - filename.filetype
	log.Println("converting..", item)
	switch srcType {
	case "pq":
		switch dstType {
		case "json":
			return pq.ConvertPqToJson(item)
		case "avro":
			log.Println("NOT IMPLEMENTED. EXITiNG")
			//return pq.ConvertPqToJson(item)
		case "csv":
			log.Println("NOT IMPLEMENTED. EXITiNG")
			//return pq.ConvertPqToJson(batch[i])
		}
	case "json":
		switch dstType {
		case "pq":
			return json.ConvertJsonToPq(item)
			//return pq.ConvertPqToJson(item)
		case "avro":
			log.Println("NOT IMPLEMENTED. EXITiNG")
			//return pq.ConvertPqToJson(item)
		case "csv":
			log.Println("NOT IMPLEMENTED. EXITiNG")
			//return pq.ConvertPqToJson(batch[i])
		}
	}
	return
}

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
	log.Println("downloaded..", file.Name(), numBytes, "bytes")
}

func regularUpload(bucket string, fileName string, filePath string) string {
	file, _ := ioutil.ReadFile(filePath)
	if len(file) > 0 {
		output, err := uploader.Upload(&s3manager.UploadInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(fmt.Sprintf("%s", fileName)),
			Body:   bytes.NewReader(file),
		})
		if err != nil {
			log.Fatalf("Unable to upload item %q, %v", filePath, err)
		}
		log.Println("uploaded..", output.Location)
		err = os.Remove(filePath) // remove file after upload
		if err != nil {
			log.Printf("Unable to remove item: %s", filePath)
		}
		return output.Location
	} else {
		log.Printf("Sorry.. Couldn't find the item to upload.\nfileName: '%s'\nfilePath: '%s'\n", fileName, filePath)
	}
	return "there was an error uploading this object.."
}

func CrawlBucket(srcBucket string) []string { // need to recursively crawl IDK if this does
	var items []string
	log.Printf("crawling %s..\n", srcBucket)
	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(srcBucket)})
	log.Println("item count:", len(resp.Contents))
	if err != nil {
		log.Printf("error crawling bucket..%s\n", err)
	}
	for _, item := range resp.Contents {
		items = append(items, *item.Key)
	}
	return items
}

func pullAndConvertBatch(srcBucket string, dstBucket string, srcType string, dstType string, batch []string) {
	defer wg.Done()
	for i := 0; i < len(batch); i++ {
		downloadObj(srcBucket, batch[i])
		fileName, filePath := conversionSwitch(srcType, dstType, batch[i])
		regularUpload(dstBucket, fileName, filePath)
	}
}

func ProcessBatches(srcBucket string, dstBucket string, srcType string, dstType string,
	fileList []string, batchSize int) {
	var j int
	for i := 0; i < len(fileList); i += batchSize {
		j += batchSize
		if j > len(fileList) { // if the upper index is > ceiling
			j = len(fileList)
		}
		wg.Add(1)
		go pullAndConvertBatch(srcBucket, dstBucket, srcType, dstType, fileList[i:j]) // begin thread
	}
	log.Println("threads started.. waiting..")
	wg.Wait()
	log.Println("threads finished.")
}
