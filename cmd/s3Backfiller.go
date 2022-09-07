package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"log"
	"s3-backfiller/pkg/s3"
	"time"
)

func convertPqToJson(srcBucket string, dstBucket string) (output string) {
	start := time.Now()
	fileList := s3.CrawlBucket(srcBucket)
	fmt.Println(fileList)
	s3.ProcessBatches(srcBucket, dstBucket, fileList, 4)
	elapsed := time.Since(start)
	log.Println("Execution time - ", elapsed) // need to print to stdout
	return output
}

func jsonToPq(srcBucket string, dstBucket string) (output string) {
	output = "need 2 implement jsonToPq"
	return output
}

var pqToJson = &cobra.Command{
	Use:     "pstojson",
	Aliases: []string{"insp"},
	Short:   "pqtojson",
	Args:    cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {

	},
}

func init() {
	rootCmd.AddCommand(pqToJson)
}
