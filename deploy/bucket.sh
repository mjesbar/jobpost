#!/usr/bin/env bash
 
path="$HOME/Github/jobpost"
log_file="$path/deploy/deploy.log"
touch $log_file

function prompt_status() {
    if [[ $? -eq 0 ]]
    then
        echo -e "OK" | tee -a $log_file
    else
        echo -e "ERROR" | tee -a $log_file
    fi
}

echo | tee -a $log_file
echo -e "STARTS BUCKET DEPLOYMENT:" | tee -a $log_file

# Getting the list of buckets, to check if bucket has been previously created
# Starts the creation process if bucket doesn't exist
echo -e "Checking if bucket exists ... " \
    | tee -a $log_file
bucket_name='jobpost-project'
bucket_check=$( aws s3 ls | grep -w "$bucket_name")


if [[ -z $bucket_check ]]
then
    # informative
    echo -e "'$bucket_name' doesn't exists, Creating Resource ... " \
        | tee -a $log_file
    echo -e "AWS response:" \
        | tee -a $log_file
    # task tracking
    echo -ne "   > AWS S3 Bucket Creation ... " \
        | tee -a $log_file
    aws s3api create-bucket --cli-input-json file://deploy/json/createBucket.json \
        >> $log_file
    prompt_status
else
    echo -e "'$bucket_name' bucket already exists in AWS account." \
        | tee -a $log_file
fi

echo -e "END BUCKET DEPLOYMENT" \
    | tee -a $log_file
