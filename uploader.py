import pandas, os, datetime, json
import boto3
print()


def describe(ls:list):
    """
    Description:
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
    Description: 
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
    def __init__(self) -> None:
        self.message = "the operation has returned an error status code"
    def __str__(self) -> str:
        return f"{self.message}"



# main program execution scope ----------------------------------------------------------------------------------------------

if __name__ == "__main__":
    
    today = datetime.date.today()
    old_partitions = list()
    new_partition = str()
    data_dir = os.listdir("./data")

    print("Describing file structure collected ... ")
    new_partition = describe(data_dir)
    print()

    print("Uploading today's file to s3://jobpost-project/raw/ S3 bucket ... ")
    response = upload_s3()
    response_status = response['ResponseMetadata']['HTTPStatusCode']
    if (response_status == 200):
        print(f"\t-> [{response_status}] Succesfully uploaded!")
    else:
        raise StatusCodeError()

    print(f"PartitionObject just uploaded: {new_partition}")

