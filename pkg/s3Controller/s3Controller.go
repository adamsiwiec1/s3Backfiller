package s3Controller

import (
	"bytes"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"io/ioutil"
	"log"
	"math"
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

func superSpeedUpload(bucketName string, fileName string, filePath string) {
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("err opening file.. %s\n", err)
		return
	}
	defer func() {
		err = file.Close()
		if err != nil {
			fmt.Printf("error closing file..%s\n", err)
		}
	}()

	fileInfo, _ := file.Stat()
	size := fileInfo.Size()
	buffer := make([]byte, size)

	_, err = file.Read(buffer)
	if err != nil {
		fmt.Printf("error reading file..%s\n", err)
	}

	path := fmt.Sprintf("%s.jsonController", fileName)
	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(path),
	}

	resp, err := svc.CreateMultipartUpload(input)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("Created multipart upload request")

	var curr, partLength int64
	var remaining = size
	var completedParts []*s3.CompletedPart
	pLen := math.Ceil(float64(size) / float64(maxPartSize))
	fmt.Println("parts:", pLen)
	ch := make(chan *s3.CompletedPart, int(pLen))
	partNumber := 1
	for curr = 0; remaining != 0; curr += partLength {
		if remaining < maxPartSize {
			partLength = remaining
		} else {
			partLength = maxPartSize
		}
		startUploadThread(svc, resp, buffer[curr:curr+partLength], partNumber, ch)
		completedPart := <-ch
		fmt.Println(completedPart)
		completedParts = append(completedParts, completedPart)
		remaining -= partLength
		partNumber++

	}
	for i := 0; i < partNumber; i++ {

	}
	completeResponse, err := completeMultipartUpload(svc, resp, completedParts)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Printf("Successfully uploaded file: %s\n", completeResponse.String())
}

func pullAndConvertBatch(srcBucket string, dstBucket string, batch []string) {
	defer wg.Done()
	for i := 0; i < len(batch); i++ {
		downloadParquet(srcBucket, batch[i])
		fileName, filePath := convertToJsonLocal(batch[i])
		//regularUpload(dstBucket, fileName, filePath)
		superSpeedUpload(dstBucket, fileName, filePath)
	}
}

func startUploadThread(svc *s3.S3, resp *s3.CreateMultipartUploadOutput, fileBytes []byte, partNumber int, ch chan<- *s3.CompletedPart) { // in development
	completedPart, err := uploadPart(svc, resp, fileBytes, partNumber)
	if err != nil {
		fmt.Println(err.Error())
		err := abortMultipartUpload(svc, resp)
		if err != nil {
			fmt.Println(err.Error())
		}
		return
	}
	ch <- completedPart
}

func uploadPart(svc *s3.S3, resp *s3.CreateMultipartUploadOutput, fileBytes []byte, partNumber int) (*s3.CompletedPart, error) {
	tryNum := 1
	partInput := &s3.UploadPartInput{
		Body:          bytes.NewReader(fileBytes),
		Bucket:        resp.Bucket,
		Key:           resp.Key,
		PartNumber:    aws.Int64(int64(partNumber)),
		UploadId:      resp.UploadId,
		ContentLength: aws.Int64(int64(len(fileBytes))),
	}

	for tryNum <= maxRetries {
		uploadResult, err := svc.UploadPart(partInput)
		if err != nil {
			if tryNum == maxRetries {
				if err2, ok := err.(awserr.Error); ok {
					return nil, err2
				}
				return nil, err
			}
			fmt.Printf("Retrying to upload part #%v\n", partNumber)
			tryNum++
		} else {
			fmt.Printf("Uploaded part #%v ETag #%s\n", partNumber, *uploadResult.ETag)
			return &s3.CompletedPart{
				ETag:       uploadResult.ETag,
				PartNumber: aws.Int64(int64(partNumber)),
			}, nil
		}
	}
	return nil, nil
}

func completeMultipartUpload(svc *s3.S3, resp *s3.CreateMultipartUploadOutput, completedParts []*s3.CompletedPart) (*s3.CompleteMultipartUploadOutput, error) {
	completeInput := &s3.CompleteMultipartUploadInput{
		Bucket:   resp.Bucket,
		Key:      resp.Key,
		UploadId: resp.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts,
		},
	}
	return svc.CompleteMultipartUpload(completeInput)
}

func abortMultipartUpload(svc *s3.S3, resp *s3.CreateMultipartUploadOutput) error {
	fmt.Println("Aborting multipart upload for UploadId#" + *resp.UploadId)
	abortInput := &s3.AbortMultipartUploadInput{
		Bucket:   resp.Bucket,
		Key:      resp.Key,
		UploadId: resp.UploadId,
	}
	_, err := svc.AbortMultipartUpload(abortInput)
	return err
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
