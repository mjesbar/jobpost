#!/usr/bin/env python3.9
from datetime import datetime
import boto3, module, pandas
import os, io, json, base64, re
print()


# interactive pandas configurations
pandas.set_option("display.max_columns", 99)
pandas.set_option("display.width", 200)

# S3 information
bucket_name = "jobpost-project"
raw_folder = "raw/"
target_folder = "processed/"


def lambda_handler(event, context):
    
    keys = list()
    tags = json.load(open("./tags.json", "r"))['tags']
    languages_columns = list(['Id']) + tags['languages']
    software_columns = list(['Id']) + tags['softwares']
    dataframe = pandas.DataFrame()
    languages = pandas.DataFrame()
    softwares = pandas.DataFrame()

    # Ingestion
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

    # getting list the 'raw/' in 'jobpost-project' bucket
    s3client = boto3.client("s3")
    response = s3client.list_objects(Bucket="jobpost-project", Marker=raw_folder)
    response_content = response.get("Contents")
    for element in response_content:
       keys.append(element.get("Key"))
    dataframe = module.merge_partitions(dataframe, keys, s3client, bucket_name)

#tmp test
    #ls_data = os.listdir("../data/")
    #ls_data_filtered = filter(lambda x: 'parquet' in x, ls_data)
    #ls = list(ls_data_filtered)

    #for file in ls:
        #tmp = f"../data/{file}"
        #tmpdf = pandas.read_parquet(tmp)
        #dataframe = pandas.concat([dataframe, tmpdf], ignore_index=True, )
    
    print("Rows collected", dataframe.shape[0])

    # Tranformation
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

    # dropping duplicated post ids
    dataframe.drop_duplicates(subset='id', inplace=True)

    # filling None values
    dataframe['company'].fillna('anon', inplace=True)
    dataframe['city'].fillna('colombia', inplace=True)
    dataframe['department'].fillna('colombia', inplace=True)

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
    dataframe['education'].fillna(pandas.NA, inplace=True)
    mask_profesional = dataframe['description'].str.match(r".*(especialista|profesional|conocimientos|sólidos)+.*")
    mask_bachilller = dataframe['description'].str.match(r".*(bachiller+|con+.?o?.?sin+.experiencia+).*")
    dataframe['education'].mask(
        (mask_profesional) & (dataframe['education']==''), tokens[2], inplace=True)
    dataframe['education'].mask(
        (mask_bachilller) & (dataframe['education']==''), tokens[1], inplace=True)
    dataframe['education'].mask(
        dataframe['education']=='', tokens[0], inplace=True)
    
    # casting the salary to integer
    dataframe['salary'] = dataframe['salary'].str.replace("$ ","", regex=False)
    dataframe['salary'] = dataframe['salary'].str.replace(",00 (Mensual)","", regex=False)
    dataframe['salary'] = dataframe['salary'].str.replace(".","", regex=False)
    mask_salary = dataframe['description'].str.extract(r".*\$.?(\d+[\.|\']\d{3}\.\d{3}) .*").squeeze()
    mask_salary = mask_salary.str.replace('$','').str.replace('.','').str.replace('\'','').str.strip()
    dataframe['salary'].update(mask_salary)
    dataframe['salary'].fillna('0', inplace=True)

    # cleaning age
    dataframe['age'].mask(
        dataframe['age'].notna(), "specific", inplace=True)
    dataframe['age'].fillna('non-specific', inplace=True)

    # cleaning type
    mask_type_hybrid1 = dataframe['description'].str.match(r".*(hibrido|[p|P]resencial y remoto)+.*")
    mask_type_hybrid2 = dataframe['type'].str.match(r"^([h|H]ibrido|[p|P]resencial y remoto)+.*")
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
    mask_experience1 = dataframe['experience'].str.extract(r"(\d)[+-]? año.?").squeeze()
    mask_experience2 = dataframe['description'].str.extract(r"(\d)[+-]?.año.?").squeeze()
    dataframe['experience'].update(mask_experience1)
    dataframe['experience'].update(mask_experience2)
    dataframe['experience'].fillna('0', inplace=True)

    # tagging english skill
    dataframe['english'] = dataframe['description'].str.match(
        r".*(advanced|english|ingl[e|é]s|experience|level|work|knowledge|team|skill.?)+.*")
    dataframe['english'].mask(
        dataframe['english']==True, 'required', inplace=True)
    dataframe['english'].mask(
        dataframe['english']==False, 'non-required', inplace=True)

    # mapping 'languages' and 'softwares' dataframes
    languages = module.tag_df(dataframe, pk='id', search='description', tags=languages_columns)
    softwares = module.tag_df(dataframe, pk='id', search='description', tags=software_columns)
    posters = dataframe.drop(columns=['title','postDate'])
    # dropping useless columns
    del dataframe

    # encoding back the 'description' columns into base64
    description_base64_index = posters['description'].index
    description_base64 = list(posters['description'])
    description_base64 = map(lambda x: x.encode('utf_8'), description_base64)
    description_base64 = map(lambda x: base64.b64encode(x), description_base64)
    posters['description'] = pandas.Series(data=list(description_base64), index=description_base64_index)

    # converting column dtypes from prior schema
    schema = json.load(open("./schema.json", "r"))
    posters = posters.astype(schema.get("posters"))
    
    # exchanging the order of 'english' and 'description' columns
    posters[['description','english']] = posters[['english','description']]
    posters.rename(columns={"description": "english", "english": "description"}, inplace=True)

    # info about the ETL
    print("Rows after ETL\n"
          "\tPosters:", posters.shape[0], "\n",
          "\tLanguages:", languages.shape[0], "\n",
          "\tSoftwares:", softwares.shape[0], "\n")
    print(posters.info())

    # Loads - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    # uploading tranformed 'dataframe'
    upload_posters_response = module.upload_df(posters, s3client=s3client,
                                               bucket=bucket_name, save_key=f"{target_folder}posters")
    upload_languages_response = module.upload_df(languages, s3client=s3client,
                                               bucket=bucket_name, save_key=f"{target_folder}languages")
    upload_softwares_response = module.upload_df(softwares, s3client=s3client,
                                               bucket=bucket_name, save_key=f"{target_folder}softwares")

    print("Uploading ... ")
    print(" > Posters DataFrame\t", upload_posters_response['ResponseMetadata']['HTTPStatusCode'])
    print(" > Languages DataFrame\t", upload_languages_response['ResponseMetadata']['HTTPStatusCode'])
    print(" > Softwares DataFrame\t", upload_softwares_response['ResponseMetadata']['HTTPStatusCode'])




lambda_handler(1,1)


