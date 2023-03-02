#!/usr/bin/env python3.9
from datetime import datetime
import boto3, module, pandas
from numpy import NaN, column_stack, nan
import os, io, json, base64, re
print()


# interactive pandas configurations
pandas.set_option("display.max_columns", 99)
pandas.set_option("display.width", 200)

# S3 information
bucket_name = "jobpost-project"
raw_folder_key = "raw/"
target_folder_key = "processed/"


def lambda_handler(event, context):
    
    keys = list()
    tags = json.load(open("./tags.json", "r"))['tags']
    dataframe = pandas.DataFrame()
    languages_columns = list(['Id']) + tags['languages']
    software_columns = list(['Id']) + tags['softwares']
    languages_df = pandas.DataFrame(columns=languages_columns)
    softwares_df = pandas.DataFrame(columns=software_columns)

    # Ingestion
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

    # getting list the 'raw/' in 'jobpost-project' bucket
    #s3client = boto3.client("s3")
    #response = s3client.list_objects(Bucket="jobpost-project", Marker=raw_folder_key)
    #response_content = response.get("Contents")
	# getting the file names
    #for element in response_content:
        #keys.append(element.get("Key"))
    # merging all the parquet partitions on bucket
    #dataframe = module.merge_df(dataframe, keys, s3client, bucket_name)

#tmp test
    ls_data = os.listdir("../data/")
    ls_data_filtered = filter(lambda x: 'parquet' in x, ls_data)
    ls = list(ls_data_filtered)

    for file in ls:
        tmp = f"../data/{file}"
        tmpdf = pandas.read_parquet(tmp)
        dataframe = pandas.concat([dataframe, tmpdf], ignore_index=True, )
    
    print("Rows collected", dataframe.shape[0])

    # Tranformation
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
    
    # dropping duplicated post ids
    dataframe.drop_duplicates(subset='id', inplace=True)
    # filling None values
    dataframe['type'].fillna('', inplace=True)
    dataframe['company'].fillna('', inplace=True)
    dataframe['experience'].fillna('', inplace=True)
    # casting the salary to integer
    dataframe['salary'] = dataframe['salary'].str.replace("$ ","", regex=False)
    dataframe['salary'] = dataframe['salary'].str.replace(",00 (Mensual)","", regex=False)
    dataframe['salary'] = dataframe['salary'].str.replace(".","", regex=False)
    dataframe['salary'].fillna('', inplace=True)
    # casting education
    dataframe['description'] = dataframe['description'].str.decode('base64_codec')
    dataframe['description'] = dataframe['description'].str.decode('utf_8')
    dataframe['description'] = dataframe['description'].str.lower()
    dataframe['education'] = dataframe['education'].str.lower()
    dataframe['education'].fillna('', inplace=True)
    tokens = ['secundaria','bachillerato','universidad']
    for (idx, token) in enumerate(tokens):
        dataframe['education'].mask(
            dataframe['education'].str.match(fr".*{token}.*"), token, inplace=True)
    mask_profesional = dataframe['description'].str.match(r".*(especialista+|profesional|conocimientos+|sólidos+).*")
    mask_bachilller = dataframe['description'].str.match(r".*(bachiller+|con+.?o?.?sin+.experiencia+).*")
    dataframe['education'].mask(
        (mask_profesional) & (dataframe['education']==''), tokens[2], inplace=True)
    dataframe['education'].mask(
        (mask_bachilller) & (dataframe['education']==''), tokens[1], inplace=True)
    dataframe['education'].mask(
        dataframe['education']=='', tokens[0], inplace=True)
    # cleaning age
    dataframe['age'].mask(
        dataframe['age'].notna(), "specific", inplace=True)
    dataframe['age'].fillna('non-specific', inplace=True)
    # cleaning type
    mask_type_hybrid1 = dataframe['description'].str.match(r".*(hibrido+|[p|P]resencial y remoto+).*")
    mask_type_hybrid2 = dataframe['type'].str.match(r"^([h|H]ibrido+|[p|P]resencial y remoto+).*")
    mask_type_hybrid = mask_type_hybrid1 | mask_type_hybrid2
    mask_type_remote1 = dataframe['description'].str.match(r".*(remoto+).*")
    mask_type_remote2 = dataframe['type'].str.match(r"^([r|R]emoto+).*")
    mask_type_remote = mask_type_remote1 | mask_type_remote2
    dataframe['type'].mask(
        mask_type_remote, "remote", inplace=True)
    dataframe['type'].mask(
        mask_type_hybrid, "hybrid", inplace=True)
    dataframe['type'].mask(
        ~(mask_type_hybrid|mask_type_remote) , "non-remote", inplace=True)
    # cleaning experience
    mask_experience1 = dataframe['experience'].str.extract(r"(\d)+ año").squeeze()
    mask_experience2 = dataframe['description'].str.extract(r"(\d)+ año").squeeze()
    dataframe['experience'].update(mask_experience1)
    dataframe['experience'].update(mask_experience2)
    # dropping useless columns
    dataframe.drop(columns=['title','postDate'], inplace=True)



    print(dataframe.info())
    print(dataframe)

    # converting column types
    #schema = json.load(open("./schema.json", "r"))
    #dataframe = dataframe.astype(schema)
    #print(dataframe.dtypes)







    #module.map_df(dataframe, 'description', languages_df.columns)

    # Loads - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    # uploading tranformed 'dataframe'
    #upload_response = module.upload_df(dataframe, s3client, bucket_name, target_folder_key)



lambda_handler(1,1)


