#!/usr/bin/env bash
 
path="$HOME/Github/jobpost"
log_file="$path/deploy/deploy.log"
touch $log_file

operation_date=$(date)
echo -e " -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  - " \
    >> $log_file
echo -e "\n > [$operation_date] :\n" >> $log_file

# Getting the list of buckets, to check if bucket has been previously created
bucket_name='jobpost-project'
bucket_check=$( aws s3 ls | grep -w "$bucket_name")

# Starts the creation process if bucket doesn't exist
echo -e "Checking if bucket $bucket_name exists ... " \
    >> $log_file

if [[ -z $bucket_check ]]
then
    echo -e "'$bucket_name' bucket doesn't exist. Creating Resource ... " \
        >> $log_file
    echo -e "AWS response:" \
        >> $log_file
    aws s3api create-bucket --cli-input-json file://json/createBucket.json \
        >> $log_file
else
    echo -e "'$bucket_name' bucket already exists in AWS account." \
        >> $log_file
fi

echo -e "END BUCKET DEPLOYMENT" \
    >> $log_file
