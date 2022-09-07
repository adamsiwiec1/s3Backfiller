package s3Backfiller

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
)

var rootCmd = &cobra.Command{
	Use:   "backfiller",
	Short: "Convert and transfer files between your S3 buckets with ease. Powered by Golang.",
	Long: `To see help text, you can run:
backfiller --help
backfiller <command> --help`,
	Run: func(cmd *cobra.Command, args []string) {

	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
