# Purpose: AWS S3 Class for C.R.U.D operations
# Author: Brian Piperato
# Created on: 10/2/2019
# Last Modified on: 10/24/2019

import os
import glob
import boto3
import json
import logging
from botocore.exceptions import ClientError, BotoCoreError
from app.modules.file_manipulation import read_directory, ProgressPercentage
from boto3.s3.transfer import TransferConfig


class s3:

    def __init__(self, _id, _secret):
        try:
            # Connects to s3 account using credentials
            self.client = boto3.client('s3',
                                       aws_access_key_id=_id,
                                       aws_secret_access_key=_secret)
        except ClientError as e:
            print(e)

    @staticmethod
    def s3_resource(_id, _secret):
        try:
            # Connects to s3 account using credentials
            resource = boto3.resource('s3', aws_access_key_id=_id, aws_secret_access_key=_secret)
            return resource
        except ClientError as e:
            return logging.error(e)

    def create_bucket(self, bucket):
        try:
            # Creates a bucket based on parameter value
            return self.client.create_bucket(Bucket=bucket)
        except BotoCoreError as e:
            return logging.error(e)

    def list_buckets(self):
        try:
            # Returns a string, dict traversed to return only bucket names associated to account based on S3 client
            result = [bucket['Name'] for bucket in self.client.list_buckets()['Buckets']]
            return result
        except BotoCoreError as e:
            return logging.error(e)

    def delete_bucket(self, bucket):
        try:
            # Delete s3 bucket and all files inside
            self.client.delete_bucket(Bucket=bucket)
        except BotoCoreError as e:
            logging.error(e)

    def read_bucket_objects(self, bucket):
        try:
            # Read bucket files and return a list
            bucket_list = [i['Key'] for i in self.client.list_objects(Bucket=bucket)['Contents']
                           if os.path.basename(i['Key']) != '']
            return bucket_list
        except BotoCoreError as e:
            return logging.error(e)

    def move_bucket_object(self, bucket, source_key, destination_key):
        # This method is used to copy a bucket file, move it to a new internal destination, and delete the original
        try:
            copy_source_key = {
                'Bucket': bucket,
                'Key': source_key
            }
            self.client.copy(copy_source_key, bucket, Key=destination_key)
            self.delete_bucket_object(bucket, source_key)
        except BotoCoreError as e:
            return logging.error(e)

    def create_bucket_policy(self, bucket):
        # Creates a bucket policy that allows all actions for the provided user based on CanonicalUser ID
        # and attaches policy to static bucket provided
        bucket_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "AddPerm",
                    "Effect": "Allow",
                    "Principal": {
                        "CanonicalUser": "d6ee2850f0cd2b777e8ace3b2a0cf6502d22e7dfb80cd7b614ae8e9113f54137"
                    },
                    "Action": ["s3:*"],
                    "Resource": ["arn:aws:s3:::{0}/*".format(bucket)]
                }
            ]
        }
        policy_string = json.dumps(bucket_policy)
        try:
            return self.client.put_bucket_policy(Bucket=bucket, Policy=policy_string)
        except BotoCoreError as e:
            logging.error(e)

    def get_bucket_policy(self, bucket):
        # Retrieves the bucket policies associated to static bucket provided
        try:
            return self.client.get_bucket_policy(Bucket=bucket)['Policy']
        except BotoCoreError as e:
            logging.error(e)

    def upload_multiple__small_files(self, _path, bucket):
        # Iterates through a given directory based on path parameter
        # Uploads each file into bucket in path directory with each iteration
        for i in glob.glob(_path + '*.*'):
            try:
                file_name = os.path.basename(i)
                self.client.upload_file(i, bucket, file_name, Callback=ProgressPercentage(i))
                print(os.path.basename(i), ' uploaded successfully')
            except BotoCoreError as e:
                logging.error(e)

    def delete_bucket_object(self, bucket, key):
        # Deletes parameter object in bucket parameter
        return self.client.delete_object(Bucket=bucket, Key=key)

    @staticmethod
    def upload_large_file(file_path, bucket_path, bucketname, _id, _secret):
        # Uploads a large file in multipart upload to increase performance
        config = TransferConfig(multipart_threshold=1024 * 25,
                                max_concurrency=10,
                                multipart_chunksize=1024 * 25,
                                use_threads=True)
        for i in read_directory(file_path):  # Checks directory for csv files
            if '.csv' in i:
                object_path = os.path.join(bucket_path, os.path.basename(i))
                try:
                    s3.s3_resource(_id, _secret).meta.client.upload_file(i, bucketname, object_path,
                                                                         ExtraArgs={'ACL': 'public-read'},
                                                                         Config=config,
                                                                         Callback=ProgressPercentage(i))
                except BotoCoreError as e:
                    logging.error(e)

    def read_through_bucket_directory(self, bucketname, directory, extension):
        # Reads through specific bucket and subdirectory for files with parameter file extension
        bucket_obj_list = self.read_bucket_objects(bucketname)
        directory_obj_list = [i for i in bucket_obj_list
                              if i.startswith(directory) and i.endswith(".{0}".format(extension))]
        return directory_obj_list

