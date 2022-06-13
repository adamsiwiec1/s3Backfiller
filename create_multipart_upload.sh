#!/bin/bash
bucketName="$1"
fileName="$2"
cd tmp
cd upload
aws s3api create-multipart-upload --bucket $bucketName --key $fileName --profile session | jq -r '.UploadId'
