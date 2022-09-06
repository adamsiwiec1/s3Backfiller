package main

import (
	"fmt"
	"log"
	"time"
)

func main() {
	/*
		Notes:
		1. Print bytes processed etc..
		2. Use log.Printf() instead of fmt, fmt interferes with the threads. do this asap.
		3. test if regular upload is more efficient for certain file sizes, find sweet spot
	*/
	start := time.Now()
	srcBucket := "s3Controller-s3Backfiller-src"
	dstBucket := "s3Controller-s3Backfiller-dst"
	fileList := crawlBucket(srcBucket)
	fmt.Println(fileList)
	processBatches(srcBucket, dstBucket, fileList, 4)
	elapsed := time.Since(start)
	log.Println("Execution time - ", elapsed)

}
