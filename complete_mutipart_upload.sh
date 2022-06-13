upload_file="$1"
bucketName="$2"
file="$3"
upload_id="$4"

aws s3api complete-multipart-upload --multipart-upload file://$upload_file --bucket $bucketName --key $file --upload-id $upload_id