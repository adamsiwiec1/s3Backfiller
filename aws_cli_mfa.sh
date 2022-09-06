# after setting credentials using this script you must run aws cli commands with the '--profile session' tag
# ex: 
# aws s3Controller cp filename.parquet s3Controller://my-s3Controller-bucket --profile session

echo "current credentials:"
cat ~/.aws/credentials
MFA_DEVICE_ARN=$(aws iam list-virtual-mfa-devices --profile default | jq -r '.[] | .[] | .SerialNumber' | grep 'adam.siwiec') # replace firstname.lastname
if [ -z "$MFA_DEVICE_ARN" ]
then
  echo "error pulling device arn.. exiting.."
else
echo $MFA_DEVICE_ARN
echo "Enter your MFA code"
read mfacode
aws sts get-session-token --serial-number $MFA_DEVICE_ARN --token-code $mfacode >> temp.json
AWS_ACCESS_KEY=$(jq -r '.Credentials.AccessKeyId' temp.json)
AWS_SECRET=$(jq -r '.Credentials.SecretAccessKey' temp.json)
AWS_SESSION_TOKEN=$(jq -r '.Credentials.SessionToken' temp.json)
TOKEN_EXPIRATION=$(jq -r '.Credentials.Expiration' temp.json)
aws configure set aws_access_key_id $AWS_ACCESS_KEY --profile session
aws configure set aws_secret_access_key $AWS_SECRET --profile session
aws configure set aws_session_token $AWS_SESSION_TOKEN --profile session
aws configure set token_expiration $TOKEN_EXPIRATION --profile session
echo "new credentials:"
cat ~/.aws/credentials
rm temp.json
fi
