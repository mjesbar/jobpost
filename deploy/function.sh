#!/usr/bin/env bash

path="$HOME/Github/jobpost"
log_file="$path/deploy/deploy.log"
touch $log_file

operation_date=$(date)
echo -e "\n > [$operation_date] :\n" >> $log_file

# Checking function exists 
function_name='cleaner'
function_check=$( aws lambda list-functions \
    | jq '.Functions[].FunctionName' -r \
    | grep -w "$function_name")

if [[ -z $function_check ]]
then
    echo -e "'$function_name' fucntion doesn't exist. Creating Resource ... " \
        >> $log_file
    echo -e "AWS response:" \
        >> $log_file
else
    echo -e "'$function_name' function already exists in AWS account." \
        >> $log_file
fi
