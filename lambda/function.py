#!/usr/bin/env python3.9

import io, pandas, json, boto3, module, time
print()

bucket_name = "jobpost-project"
raw_folder_key = "raw/"
target_folder_key = "processed/"

def lambda_handler():
    
    keys = list()
    dataframe = pandas.DataFrame()
	
    # getting list the 'raw/' in 'jobpost-project' bucket
    s3client = boto3.client("s3")
    response = s3client.list_objects(Bucket="jobpost-project", Marker=raw_folder_key)
    response_content = response.get("Contents")
	# getting the file names
    for element in response_content:
        keys.append(element.get("Key"))
    # merging all the parquet partitions on bucket
    dataframe = module.merge_df(dataframe, keys, s3client, bucket_name)

    # Tranformations - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
    print(dataframe.tail(10))

    # uploading tranformed 'dataframe'
    #upload_response = module.upload_df(dataframe, s3client, bucket_name, target_folder_key)


lambda_handler()


