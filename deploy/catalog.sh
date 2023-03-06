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

echo | tee -a $log_file
echo -e "STARTS GLUE CATALOG DEPLOYMENT:" | tee -a $log_file

# Getting the databases created on glue catalog, if both the database and 
# table has not been created, so proceed to do it.
database='jobpost'
databases_list=$( aws glue get-databases | jq '.DatabaseList[].Name' -r )
database_check=$( echo $databases_list | grep -w "$database")
tables=( 'posters' 'softwares' 'languages' )

# creating the Database
if [[ -z $database_check ]]
then
    #informative
    echo -e "'$database' doesn't exists, Creating Resource ... " \
        | tee -a $log_file
    echo -e "AWS response:" \
        | tee -a $log_file
    # task tracking
    echo -ne "   > AWS Glue Database in Catalog Creation ... " \
        | tee -a $log_file
    aws glue create-database --cli-input-json file://$path/deploy/json/createDatabase.json \
        >> $log_file
    prompt_status

    echo -ne "   > AWS Glue Tables in $database DB Creation ... " \
        | tee -a $log_file
    for table in ${tables[@]}
    do
        aws glue create-table --cli-input-json file://deploy/schemas/${table}Schema.json \
        >> $log_file
    done 
    prompt_status
else
    echo -e "'$database' database already exists in AWS account." \
        | tee -a $log_file
fi

echo -e "END BUCKET DEPLOYMENT" \
    | tee -a $log_file

