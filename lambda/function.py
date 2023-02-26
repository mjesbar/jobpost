	#!/usr/bin/env python3.9

import io
import pandas
import json
import boto3
import module
print()

def lambda_handler():

    bucket_name = "jobpost-project"
    raw_folder_key = "raw/"
    target_folder_key = "processed/"
    keys = list()
    dataframe = pandas.DataFrame()

	# getting list the 'raw/' in 'jobpost-project' bucket
    s3client = boto3.client("s3")
    response = s3client.list_objects(Bucket="jobpost-project", Marker=raw_folder_key)

	# ispecting the content key
    response_content = response.get("Contents")

	# getting the file names
    for element in response_content:
        keys.append(element.get("Key"))

	# getting the binaries and append to 'dataframe'
    for (n,key) in enumerate(keys):
        object_response = s3client.get_object(Bucket=bucket_name,
                                   Key=key)
        status = object_response.get("ResponseMetadata").get("HTTPStatusCode")
        object_body = object_response.get("Body")
        if (n < 1):
            parquet_file = module.s3object_to_binary(object_body)
            dataframe = pandas.read_parquet(parquet_file, engine="pyarrow")
            print(dataframe.shape)
        else:
            parquet_file = module.s3object_to_binary(object_body)
            df = pandas.read_parquet(parquet_file, engine="pyarrow")
            dataframe = pandas.concat([dataframe, df], ignore_index=True, verify_integrity=True)
            print(dataframe.shape)
	#end for
    
    print(dataframe.count() * dataframe.memory_usage())


	# 'dataframe' tranformations -----------------------------------------------------------------------------
    

    # tranformed 'dataframe' upload
    module.upload(dataframe, s3client, bucket_name, target_folder_key)

lambda_handler()



