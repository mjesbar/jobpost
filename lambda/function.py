#!/usr/bin/env python3.9

import pandas
import json
import boto3
print()


bucket_name = "jobpost-project"
subfolder_key = "raw/"
keys = list()

# getting list the 'raw/' in 'jobpost-project' bucket
s3client = boto3.client("s3")
response = s3client.list_objects(Bucket="jobpost-project", Marker=subfolder_key)
response_pretty = json.dumps(response, indent=4, default=str)
# print(response_pretty)

# ispecting the content key
response_content = response.get("Contents")
response_pretty = json.dumps(response_content, indent=4, default=str)
#print(response_pretty)

# getting the file names
for element in response_content:
    keys.append(element.get("Key"))
#print(keys)

# getting the binaries and append to 'dataframe'
for key in keys:
    object_response = s3client.get_object(Bucket=bucket_name,
                               Key=key)
    response_pretty = json.dumps(object_response, indent=4, default=str)
    print(response_pretty)
    status = object_response.get("ResponseMetadata")







