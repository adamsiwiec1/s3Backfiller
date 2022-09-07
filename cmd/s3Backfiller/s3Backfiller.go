package s3Backfiller

import (
	"github.com/spf13/cobra"
	"s3Backfiller/pkg/s3"
)

//var jsonToPq = &cobra.Command{
//	Use: "json2pq",
//	//Aliases: []string{"pqtojson"},
//	Short: "j2p",
//	Run: func(cmd *cobra.Command, args []string) {
//		// Ask
//		srcBucket, _ := cmd.PersistentFlags().GetString("srcBucket")
//		dstBucket, _ := cmd.PersistentFlags().GetString("dstBucket")
//		batches, _ := cmd.PersistentFlags().GetInt("batchSize")
//		// Act
//		items := s3.CrawlBucket(srcBucket)
//		s3.ProcessBatches(srcBucket, dstBucket, items, batches)
//		// Assert
//		// Need 2 Implement
//	},
//}

var pqToJson = &cobra.Command{
	Use: "pq2json",
	//Aliases: []string{"pqtojson"},
	Short: "p2j",
	Run: func(cmd *cobra.Command, args []string) {
		// Ask
		srcBucket, _ := cmd.PersistentFlags().GetString("srcBucket")
		dstBucket, _ := cmd.PersistentFlags().GetString("dstBucket")
		srcType, _ := cmd.PersistentFlags().GetString("srcType")
		dstType, _ := cmd.PersistentFlags().GetString("dstType")
		batches, _ := cmd.PersistentFlags().GetInt("batchSize")
		// Act
		items := s3.CrawlBucket(srcBucket)
		s3.ProcessBatches(srcBucket, dstBucket, srcType, dstType, items, batches)
		// Assert
		// Need 2 Implement
	},
}

func init() {
	rootCmd.AddCommand(pqToJson)
	pqToJson.PersistentFlags().String("srcBucket", "", "Source bucket to pull data from.")
	pqToJson.PersistentFlags().String("dstBucket", "", "Destination bucket to load converted data.")
	pqToJson.PersistentFlags().String("srcType", "", "Source bucket to pull data from.")
	pqToJson.PersistentFlags().String("dstType", "", "Destination bucket to load converted data.")
	pqToJson.PersistentFlags().Int("batchSize", 1, "Number of items to process concurrently. (min 2/max 10)")
}
