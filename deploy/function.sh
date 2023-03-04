#!/usr/bin/env bash

path="$HOME/Github/jobpost"
log_file="$path/deploy/deploy.log"
touch $log_file

operation_date=$(date)
echo -e " -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  - " \
    >> $log_file
echo -e "\n > [$operation_date] :\n" \
    >> $log_file

# Checking function exists 
function_name='cleaner'
function_check=$( aws lambda list-functions \
    | jq '.Functions[].FunctionName' -r \
    | grep -w "$function_name")

# Starts the creation process if function doesn't exist
echo -e "Checking if bucket $function_name exists ... " \
    >> $log_file

if [[ -z $function_check ]]
then
    echo -e "'$function_name' fucntion doesn't exist. Creating Resource ... " \
        >> $log_file
    echo -e "AWS response:" \
        >> $log_file
    # for some reason there's no way to upload the blob 'cleaner.zip' file via
    # json-cli, Maybe lack of base64 support. Anyway, I use the shothand way
    # suggested by aws.
    echo -e "   > AWS Lambda Function Creation ... " \
        >> $log_file
    aws lambda create-function \
        --function-name "$function_name" \
        --runtime "python3.9" \
        --zip-file "fileb://$path/lambda/functions/cleaner/cleaner.zip" \
        --layers "arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python39:3" \
        --package-type "Zip" \
        --handler "code.lambda_handler" \
        --role "arn:aws:iam::326117943082:role/AdminRole" \
        --timeout 30 \
        --memory-size 512 \
        --publish \
        >> $log_file

    echo -e "   > AWS Lambda Function Trigger Settings ..." \
        >> $log_file
    aws s3api put-bucket-notification-configuration --cli-input-json file://deploy/json/s3EventConfiguration.json \
        >> $log_file

    echo -e "   > AWS Lambda Function Permissions Settings ..." \
        >> $log_file
    aws lambda add-permissions --cli-input-json file://deploy/json/s3EventPermission.json \
        >> $log_file
else
    echo -e "'$function_name' function already exists in AWS account." \
        >> $log_file
fi

echo -e "END FUNCTION DEPLOYMENT" \
    >> $log_file
