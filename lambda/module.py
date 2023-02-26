"""
    this module provide the custom function used to ease the data streaming 
    manipulation overall.
"""

import io, os
from pandas import DataFrame
from botocore.response import StreamingBody


def s3object_to_binary(obj : StreamingBody):
    # converting StreamingBody s3 custom binary type to Python BufferedType
    obj_streaming_body = obj
    obj_bytes = obj.read()
    buffer_bytes = io.BytesIO(obj_bytes)
    
    return buffer_bytes


def upload(data : DataFrame, s3client, bucket, target_path):   
	# exporting tranformed dataframe toward target subfolder in 'jobpost-project' Bucket
    save_bytes = open("./tmp", "wb")
    data.to_parquet(save_bytes, engine="pyarrow", compression="gzip")

    upload_bytes = open("./tmp", "rb")
    s3client.put_object(Body=upload_bytes, Bucket=bucket,
                        Key=f"{target_path}test.gz.parquet", ContentType="parquet")
    
    save_bytes.close()
    upload_bytes.close()
    os.remove("./tmp")
            




