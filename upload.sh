uploadId="$1"
bucketName="$2"
filename="$3"
filetype="$4"
partNumber="$5"
cd tmp
cd upload
aws s3api upload-part --bucket $bucketName --key $filename$filetype --upload-id $uploadId --part-number $partNumber --body $filename$partNumber$filetype --profile session



