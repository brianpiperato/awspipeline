# Purpose: AWS data etl script including functions to manipulate data
# Author: Brian Piperato
# Created on: 10/2/2019
# Last Modified on: 10/17/2019

import os
import glob
import boto3
import json
import logging
from botocore.exceptions import ClientError, BotoCoreError
from app.progressbar_class import ProgressPercentage


class s3:

    def __init__(self, _id, _secret):
        try:
            # Connects to s3 account using credentials
            self.client = boto3.client('s3',
                              aws_access_key_id=_id,
                              aws_secret_access_key=_secret)
        except ClientError as e:
            return logging.error(e)

    @classmethod
    def s3_resource(cls, _id, _secret):
        try:
            # Connects to s3 account using credentials
            cls.resource = boto3.resource('s3', aws_access_key_id=_id, aws_secret_access_key=_secret)
        except ClientError as e:
            return logging.error(e)

    def create_bucket(self, bucket):
        # Creates a bucket based on parameter value
        return self.client.create_bucket(Bucket=bucket)

    def list_buckets(self):
        try:
            # Returns a string, dict traversed to return only bucket names associated to account based on S3 client
            result = '\n'.join([bucket['Name'] for bucket in self.client.list_buckets()['Buckets']])
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
            bucket_list = '\n'.join([i['Key'] for i in self.client.list_objects(Bucket=bucket)['Contents']
                                     if os.path.basename(i['Key']) != ''])
            return bucket_list
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
                    "Resource": ["arn:aws:s3:::{0}/*".format(bucket_name)]
                }
            ]
        }
        policy_string = json.dumps(bucket_policy)
        return self.client.put_bucket_policy(Bucket=bucket, Policy=policy_string)

    def get_bucket_policy(self, bucket):
        # Retrieves the bucket policies associated to static bucket provided
        return self.client.get_bucket_policy(Bucket=bucket)['Policy']

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


# bucket_name = 'abh-faa-stage-bp'
# data_directory = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data/')
# s3_class = s3(account_id, secret)
# s3_class.create_bucket('testabh19')
# s3_class.delete_bucket('testabh19')
# print(s3_class.read_bucket_objects('abh-faa-stage-bp'))
# print(s3_class.get_bucket_policy('abh-faa-stage-bp'))
# s3_class.upload_multiple__small_files(data_directory, bucket_name)
# s3_class.delete_bucket_object(bucket_name, 'snowflake_connection_class.py')
# print(s3_class.read_bucket_objects(bucket_name))
    #
    # def upload_large_file(bucketname):
    #
    #     config = TransferConfig(multipart_threshold=1024 * 25,
    #                             max_concurrency=10,
    #                             multipart_chunksize=1024 * 25,
    #                             use_threads=True)
    #
    #     file_path = None
    #     for i in read_directory(data_directory):
    #         if '.csv' in i:
    #             file_path = i
    #     object_path = os.path.join('FAA/Source Data', os.path.basename(file_path))
    #
    #     s3_resource().meta.client.upload_file(file_path, bucketname, object_path,
    #                                           ExtraArgs={'ACL': 'public-read'},
    #                                           Config=config,
    #                                           Callback=ProgressPercentage(file_path))
    #
    # def read_files_from_bucket_and_execute_sql(bucketname, directory):
    #     comp_counter = 0
    #     fail_counter = 0
    #     for _file in read_through_bucket_directory(bucketname, directory):
    #         obj = s3_resource().Object(bucketname, _file)
    #         body = obj.get()['Body'].read().decode('utf-8')  # Reads the s3 object and returns a string
    #         post_body = re.sub(r'(?m)^ *--.*\n?', '', body)  # Removes commented lines from text
    #         sql = [i for i in post_body.split(';')]  # Splits string into list elements to be run consecutively
    #         for i in sql:
    #             if i != '\r\n':  # Reading a text file causes carriage return, removes text that isn't sql
    #                 try:
    #                     print(i)
    #                     sf.query(i)
    #                     print('Completed!')
    #                     comp_counter += 1
    #                 except:
    #                     user_input = input('FAILURE! Do you wish to continue? (y/n): ').lower()
    #                     if user_input == 'n':
    #                         sys.exit(1)
    #                     else:
    #                         fail_counter += 1
    #         print('{0} processing Complete! \n Checking for additional files...'.format(_file))
    #     sf.close()
    #     return print('Completed tasks: ', comp_counter, '\nFailed Tasks: ', fail_counter)
    #
    # def read_through_bucket_directory(bucketname, directory):
    #     bucket_obj_list = read_bucket_objects(bucketname)
    #     directory_obj_list = [i for i in bucket_obj_list if i.startswith(directory) and i.endswith(".sql")]
    #     return directory_obj_list
