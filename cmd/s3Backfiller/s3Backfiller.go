package s3Backfiller

import (
	"fmt"
	"github.com/spf13/cobra"
	"log"
	"s3Backfiller/pkg/s3"
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
	Use: "pqtojson",
	//Aliases: []string{"pqtojson"},
	Short: "p2j",
	Run: func(cmd *cobra.Command, args []string) {
		// Ask
		srcBucket, _ := cmd.PersistentFlags().GetString("srcBucket")
		dstBucket, _ := cmd.PersistentFlags().GetString("dstBucket")
		batches, _ := cmd.PersistentFlags().GetInt("batchSize")
		// Act
		items := s3.CrawlBucket(srcBucket)
		s3.ProcessBatches(srcBucket, dstBucket, items, batches)
	},
}

func init() {
	rootCmd.AddCommand(pqToJson)
	pqToJson.PersistentFlags().String("srcBucket", "", "Source bucket to pull data from.")
	pqToJson.PersistentFlags().String("dstBucket", "", "Destination bucket to load converted data.")
	pqToJson.PersistentFlags().Int("batchSize", 1, "Number of items to process concurrently. (min 2/max 10)")
}
