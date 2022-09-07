package main

import (
	"log"
	"os"
	"s3Backfiller/cmd/s3Backfiller"
)

func main() {
	// create necessary directories and files on startup
	if _, err := os.Stat("/tmp/log/s3Backfiller.log"); os.IsNotExist(err) {
		os.MkdirAll("/tmp/log", 0700)
		os.MkdirAll("/tmp/dst", 0700)
		os.MkdirAll("/tmp/src", 0700)
	}
	//file, _ := os.Create("~/var/log/s3Backfiller.log") // create in /var/log for production
	os.Create("tmp/log/s3Backfiller.log")
	logFile, err := os.OpenFile("tmp/log/s3Backfiller.log", os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Println("Error accessing log file. **should exit on this")
	}
	defer logFile.Close()
	log.SetOutput(logFile)
	s3Backfiller.Execute()
}
