package main

import "s3Backfiller/pkg/json"

func main() {
	// create necessary directories and files on startup
	//if _, err := os.Stat("/tmp/log/s3Backfiller.log"); os.IsNotExist(err) {
	//	os.MkdirAll("var/log", 0700) // create in /var/log/s3Backfiller/ and /tmp/S3Backfiller/ for production
	//	os.MkdirAll("tmp/dst", 0700)
	//	os.MkdirAll("tmp/src", 0700)
	//}
	//os.Create("var/log/s3Backfiller.log")
	//logFile, err := os.OpenFile("tmp/log/s3Backfiller.log", os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	//if err != nil {
	//	log.Println("Error accessing log file. **should exit on this")
	//}
	//defer logFile.Close()
	//log.SetOutput(logFile)
	//s3Backfiller.Execute()
	json.ConvertJsonToPq("tmp/dst/green_tripdata_2022-01.json")
}
