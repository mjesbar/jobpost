import io, os, boto3, base64
from pandas import DataFrame, Index, Series, read_parquet, read_csv, concat
from botocore.response import StreamingBody


"""
    this module provide the custom function used to ease along the data streaming and 
    manipulation overall.
"""


def s3object_bytes(obj : StreamingBody):
    # converting StreamingBody s3 custom binary type to Python BufferedType
    obj_streaming_body = obj
    obj_bytes = obj.read()
    buffer_bytes = io.BytesIO(obj_bytes)
    
    return buffer_bytes


def upload_df(data : DataFrame, s3client, bucket, target_path):   
	# exporting tranformed dataframe toward target subfolder in 'jobpost-project' Bucket
    # save step
    save_bytes = open("./tmp", "wb")
    data.to_parquet(save_bytes, engine="pyarrow", compression="gzip")
    # upload step
    upload_bytes = open("./tmp", "rb")
    response = s3client.put_object(Body=upload_bytes, Bucket=bucket,
                                   Key=f"{target_path}test.gz.parquet", ContentType="parquet")
    save_bytes.close()
    upload_bytes.close()
    os.remove("./tmp")

    return response
            

def merge_df(df : DataFrame, keys : list[str], s3, bucket):
	# getting the binaries and append to 'df'
    for (n,key) in enumerate(keys):
        object_response = s3.get_object(Bucket=bucket, Key=key)
        status = object_response.get("ResponseMetadata").get("HTTPStatusCode")
        object_body = object_response.get("Body")
        if (n < 1):
            parquet_file = s3object_bytes(object_body)
            df = read_parquet(parquet_file, engine="pyarrow")
        else:
            parquet_file = s3object_bytes(object_body)
            tmpdf = read_parquet(parquet_file, engine="pyarrow")
            df = concat([df, tmpdf], ignore_index=True, verify_integrity=True)
	#end for
    print("\rRows Appended", df.shape[0])

    return df


def map_df(df : DataFrame, column : str, tags : Index):
    # mapping 'map_columns' through df column
    df_content = df['description']
    for str_encoded in df_content:
        post_str = base64.b64decode(str_encoded)
        post_str = post_str.decode('utf8')
    
    return 0


   




