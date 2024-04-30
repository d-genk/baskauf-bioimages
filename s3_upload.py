import os
import boto3
from datetime import datetime

def upload_directory_to_s3(local_directory, bucket_name):
    # Create an S3 client
    s3_client = boto3.client('s3')

    # Walk through the local directory
    for root, dirs, files in os.walk(local_directory):
        for filename in files:
            # Create the full local path
            local_path = os.path.join(root, filename)

            # Retrieve the last modified time of the file
            last_modified_time = os.stat(local_path).st_mtime
            readable_time = datetime.fromtimestamp(last_modified_time).strftime('%Y-%m-%d %H:%M:%S')

            # Create the S3 path by removing the leading local directory path and replacing
            # os path separator with '/', which S3 uses
            s3_path = os.path.relpath(local_path, local_directory).replace(os.sep, '/')

            # Upload the file with custom metadata for the last modified date
            print(f"Uploading {local_path} to {bucket_name}/{s3_path} with last modified date {readable_time}")
            s3_client.upload_file(local_path, bucket_name, s3_path, ExtraArgs={
                'Metadata': {'last_modified': readable_time}
            })

# Usage
'''local_directory = 'F:/bioimages'
bucket_name = 'baskauf-bioimages'
upload_directory_to_s3(local_directory, bucket_name)'''

def count_objects_in_bucket(bucket_name):
    # Create an S3 client
    s3_client = boto3.client('s3')
    count = 0

    # Use the paginator to handle buckets with many objects
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name)

    for page in pages:
        if 'Contents' in page:
            count += len(page['Contents'])

    return count

# Usage
'''bucket_name = 'baskauf-bioimages'
total_objects = count_objects_in_bucket(bucket_name)
print(f"Total objects in bucket '{bucket_name}': {total_objects}")'''
