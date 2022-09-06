module s3-backfiller

go 1.18

require (
	github.com/aws/aws-sdk-go v1.44.32
	github.com/spf13/cobra v1.5.0
	github.com/xitongsys/parquet-go v1.6.2
	github.com/xitongsys/parquet-go-source v0.0.0-20220527110425-ba4adb87a31b
)

require github.com/inconshreveable/mousetrap v1.0.1 // indirect

replace github.com/xitongsys/parquet-go v1.6.2 => github.com/adamsiwiec1/parquet-go v1.6.2
