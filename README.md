# s3Backfiller

**ETL Tool written in Golang that extracts data from S3, transforms it to another file format, and loads back into S3.**

### Supported file formats:
- Parquet
- JSON
- CSV (not yet implemented)
- Avro (not yet implemented)
- XML (not yet implemented)
- ORC (not yet implemented)
- Base64 (not yet implemented)
- Raw (not yet implemented)

### Current Features:
- `pq2json` - Convert Parquet to JSON between Amazon S3 Buckets using Golang.

### Features to be implemented:
- Write verbose status to log file and output % complete to stdout (along w/ completion time).
- Convert JSON to Parquet between Amazon S3 Buckets using Golang. (next PR)
- Add flag to optionally zip dest files.
- Option to organize destination folder structure. 
- Automate batch size selection to be most efficient. 
- Add commands to clean up /tmp dir upon exit.
- "How would you like to deal with nested json?" prompt. (i.e., create a new pq file and append to it each time you encounter a nested field, write the nested field all into 1 cell as a string, etc..)

### Examples:

`call to script`

