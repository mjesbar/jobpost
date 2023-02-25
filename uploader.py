from re import A
import pandas, os, datetime, json
import boto3
from pandas.core.dtypes.common import classes
print()


def describe(ls:list):
    """
    fDescription:
    list the batches that we've already saved from scraping.
    """
    partition = str()
 
    for file in data_dir:
        if ('parquet' in file) & (f'{today}' in file):
            partition = file
            print("new partition:\n\t", partition)

    print("==============================================\nold_partitions:")
    for file in data_dir:
        if ('parquet' in file) & (f'{today}' not in file):
            old_partitions.append(file)
            print("\t",old_partitions[-1])

    return partition



def upload_s3():
    """
    fDescription: 
    upload using the low-level API function from boto3 with a S3 client.
    """    
    s3 = boto3.client('s3')
    new_partition_binary = open(f"data/{new_partition}", 'rb')

    response = s3.put_object(Body=new_partition_binary,
                             Bucket='jobpost-project',
                             Key=f'raw/{new_partition}',
                             ContentType='parquet')
    return response



class StatusCodeError(Exception):
    def __init__(self, message) -> None:
        self.message = "the opoeration has returned an error status code"
        super().__init__(self.message)



# main program execution scope ----------------------------------------------------------------------------------------------

if __name__ == "__main__":
    
    today = datetime.date.today()
    old_partitions = list()
    new_partition = str()
    data_dir = os.listdir("./data")

    print("Describing file structure collected ... ")
    new_partition = describe(data_dir)
    print()

    print("Uploading today's file to s3://jobpost-project/raw/ S3 bucket ... ", end='')
    response = upload_s3()
    response_status = response['ResponseMetadata']['HTTPStatusCode']
    if (response_status == 200):
        print(" -> ", response_status)
    else:
        raise StatusCodeError(" -> StatusCodeError")
    print("PartitionObject just uploaded:", new_partition)

