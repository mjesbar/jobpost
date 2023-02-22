from hashlib import new
import pandas, os, datetime
import boto3
print()


def describe(ls:list):

    partition = str()

    for file in data_dir:
        if ('csv' in file) & (f'{today}' in file):
            partition = file
            print("new partition:\n\t", partition)
            print("==============================================\nold_partitions:")

    for file in data_dir:
        if ('csv' in file) & (f'{today}' not in file):
            old_partitions.append(file)
            print("\t",old_partitions[-1])

    return partition


def upload_s3():
    
    s3 = boto3.client('s3')
    response = s3.put_object(Body=f'data/{new_partition}',
                             Bucket='jobpost-project',
                             Key=f'{new_partition}')
    return str(response)


if __name__ == "__main__":
    
    today = datetime.date.today()
    old_partitions = list()
    new_partition = str()
    data_dir = os.listdir('data/')

    print("Describing file structure collected ... ")
    new_partition = describe(data_dir)
    print()

    print("Uploading today's file to s3://jobpost-project/ S3 bucket ... ")
    print(new_partition)
    upload_s3()

