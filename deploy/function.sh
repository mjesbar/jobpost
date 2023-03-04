#!/usr/bin/env bash

path="$HOME/Github/jobpost"
log_file="$path/deploy/deploy.log"
touch $log_file

function prompt_status() {
    if [[ $? -eq 0 ]]
    then
        echo -e "OK" | tee -a $log_file
    fi
}

echo -e "STARTS FUNCTION DEPLOYMENT :" | tee -a $log_file

# Checking function exists 
function_name='cleaner'
function_check=$( aws lambda list-functions \
    | jq '.Functions[].FunctionName' -r \
    | grep -w "$function_name")

# Starts the creation process if function doesn't exist
echo -e "Checking if function $function_name exists ... " \
    | tee -a $log_file

if [[ -z $function_check ]]
then
    echo -e "'$function_name' doesn't exists, Creating Resource ... " \
        | tee -a $log_file
    echo -e "AWS response:" \
        | tee -a $log_file
    # for some reason there's no way to upload the blob 'cleaner.zip' file via
    # json-cli, Maybe lack of base64 support. Anyway, I use the shothand way
    # suggested by aws.
    echo -ne "   > AWS Lambda Function Creation ... " \
        | tee -a $log_file
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
    prompt_status

    echo -ne "   > AWS Lambda Function Trigger Settings ... " \
        | tee -a $log_file
    aws s3api put-bucket-notification-configuration \
        --cli-input-json file://deploy/json/s3EventConfiguration.json \
        >> $log_file
    prompt_status

    echo -ne "   > AWS Lambda Function Permissions Settings ... " \
        | tee -a $log_file
    aws lambda add-permission \
        --cli-input-json file://deploy/json/s3EventPermission.json \
        >> $log_file
    prompt_status
else
    echo -e "'$function_name' function already exists in AWS account." \
        | tee -a $log_file
fi

echo -e "END FUNCTION DEPLOYMENT" \
    | tee -a $log_file


