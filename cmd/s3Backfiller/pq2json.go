package s3Backfiller

import (
	"github.com/spf13/cobra"
	"log"
	"s3Backfiller/pkg/s3"
)

var pqToJson = &cobra.Command{
	Use: "pq2json",
	Run: func(cmd *cobra.Command, args []string) {
		// Ask
		srcBucket, _ := cmd.PersistentFlags().GetString("srcBucket")
		dstBucket, _ := cmd.PersistentFlags().GetString("dstBucket")
		batches, _ := cmd.PersistentFlags().GetInt("batchSize")
		srcType := "pq"
		dstType := "json"
		// Act
		items := s3.CrawlBucket(srcBucket)
		// verify items are of the correct type, grab only items of correct type - method
		s3.ProcessBatches(srcBucket, dstBucket, srcType, dstType, items, batches)
		// Assert
		log.Println("Need to implement pq2json assertion.")
	},
}

func init() {
	rootCmd.AddCommand(pqToJson)
	pqToJson.PersistentFlags().String("srcBucket", "", "Source bucket to pull data from.")
	pqToJson.PersistentFlags().String("dstBucket", "", "Destination bucket to load converted data.")
	pqToJson.PersistentFlags().Int("batchSize", 1, "Number of items to process concurrently. (min 2/max 10)")
}
