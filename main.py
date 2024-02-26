import os
import boto3
from boto3.s3.transfer import TransferConfig

# Get user input for content directory
local_directory = '/Users/mashoodoptera/Documents/test-content'

# Get user input for S3 prefix
# s3_prefix = input("Enter S3 prefix: ")
s3_prefix = 'aplus/sony-temp/'

# S3 Configuration
s3_bucket = 'beginvideocontent'

# Connect to S3
s3_client = boto3.client('s3')


# Function to upload a file to S3 using multipart upload if size is 5GB or above
def upload_to_s3(local_file, s3_key):
    s3_client = boto3.client('s3')

    # Check file size
    file_size = os.path.getsize(local_file)

    try:
        if file_size >= 7 * 1024 * 1024 * 1024:  # 7GB
            print(f'Uploading {local_file} using multipart upload')

            # Multipart upload configuration
            config = TransferConfig(multipart_threshold=5 * 1024 * 1024 * 1024)

            # Initiate multipart upload
            response = s3_client.create_multipart_upload(Bucket=s3_bucket, Key=s3_key)
            upload_id = response['UploadId']

            # Upload parts
            with open(local_file, 'rb') as data:
                parts = []
                part_number = 1
                while True:
                    chunk = data.read(config.multipart_chunksize)
                    if not chunk:
                        break
                    response = s3_client.upload_part(Body=chunk, Bucket=s3_bucket, Key=s3_key, UploadId=upload_id,
                                                     PartNumber=part_number)
                    parts.append({'PartNumber': part_number, 'ETag': response['ETag']})
                    part_number += 1

            # Complete multipart upload
            s3_client.complete_multipart_upload(Bucket=s3_bucket, Key=s3_key, UploadId=upload_id,
                                                MultipartUpload={'Parts': parts})
        else:
            print(f'Uploading {local_file} using regular upload')
            s3_client.upload_file(local_file, s3_bucket, s3_key)
    except Exception as e:
        print(e)


# get the list of local file names
local_files = os.listdir(local_directory)

# Connect to S3
s3_client = boto3.client('s3')

# Iterate through LOCAL files and synchronize with S3
for file in local_files:
    s3_bucket_key = s3_prefix + file
    file_path = local_directory + '/' + file
    upload_to_s3(file_path, s3_bucket_key)
