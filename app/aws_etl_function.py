# Purpose: AWS data etl script including functions to manipulate data
# Author: Brian Piperato
# Created on: 10/2/2019
# Last Modified on: 10/17/2019

import sys
import re
import zipfile
# noinspection PyUnresolvedReferences
from app.file_manipulation import *
from boto3.s3.transfer import TransferConfig
# noinspection PyUnresolvedReferences
from progressbar_class import ProgressPercentage
# noinspection PyUnresolvedReferences
from snowflake_connection_class import sf
from app.data_pull import web_crawler


#
# def s3_client():
#     try:
#         # Connects to s3 account using credentials
#         s3 = boto3.client('s3',
#                           aws_access_key_id=_id,
#                           aws_secret_access_key=secret)
#     except ClientError as e:
#         return logging.error(e)
#     return s3
#
#
# def s3_resource():
#     try:
#         # Connects to s3 using credentials
#         s3 = boto3.resource('s3',
#                             aws_access_key_id=_id,
#                             aws_secret_access_key=secret)
#     except ClientError as e:
#         return logging.error(e)
#     return s3

#
# def create_bucket(bucketname):
#     # Creates a bucket based on parameter value
#     return s3_client().create_bucket(Bucket=bucketname)
#
#
# def list_buckets():
#     # Returns a dict, traversed to return only bucket names associated to account based on S3 client
#     return s3_client().list_buckets()['Buckets'][0]['Name']
#
#
# def read_bucket_objects(bucketname):
#     # Read bucket files with no parent directory and return a list
#     bucket_list = [i['Key'] for i in s3_client().list_objects(Bucket=bucketname)['Contents']]
#     return bucket_list
#
#
# def create_bucket_policy():
#     # Creates a bucket policy that allows all actions for the provided user based on CanonicalUser ID
#     # and attaches policy to static bucket provided
#     bucket_policy = {
#         "Version": "2012-10-17",
#         "Statement": [
#             {
#                 "Sid": "AddPerm",
#                 "Effect": "Allow",
#                 "Principal": {
#                     "CanonicalUser": "d6ee2850f0cd2b777e8ace3b2a0cf6502d22e7dfb80cd7b614ae8e9113f54137"
#                 },
#                 "Action": ["s3:*"],
#                 "Resource": ["arn:aws:s3:::{0}/*".format(bucket_name)]
#             }
#         ]
#     }
#
#     policy_string = json.dumps(bucket_policy)
#
#     return s3_client().put_bucket_policy(Bucket=bucket_name, Policy=policy_string)
#
#
# def get_bucket_policy():
#     # Retrieves the bucket policies associated to static bucket provided
#     return s3_client().get_bucket_policy(Bucket=bucket_name)['Policy']
#
#
# def upload_multiple__small_files(path):
#     # Iterates through a given directory based on path parameter
#     # Uploads each file in directory with each iteration
#     for i in glob.glob(path):
#         try:
#             file_name = os.path.basename(i)
#             s3_client().upload_file(i, bucket_name, file_name, callback=ProgressPercentage(i))
#             print(os.path.basename(i), ' uploaded successfully')
#         except:
#             print('Upload Error when trying to load file: ', os.path.basename(i))
#

def upload_large_file(bucketname):

    config = TransferConfig(multipart_threshold=1024 * 25,
                            max_concurrency=10,
                            multipart_chunksize=1024 * 25,
                            use_threads=True)

    file_path = None
    for i in read_directory(data_directory):
        if '.csv' in i:
            file_path = i
    object_path = os.path.join('FAA/Source Data', os.path.basename(file_path))

    s3_resource().meta.client.upload_file(file_path, bucketname, object_path,
                                          ExtraArgs={'ACL': 'public-read'},
                                          Config=config,
                                          Callback=ProgressPercentage(file_path))


def read_files_from_bucket_and_execute_sql(bucketname, directory):
    comp_counter = 0
    fail_counter = 0
    for _file in read_through_bucket_directory(bucketname, directory):
        obj = s3_resource().Object(bucketname, _file)
        body = obj.get()['Body'].read().decode('utf-8')  # Reads the s3 object and returns a string
        post_body = re.sub(r'(?m)^ *--.*\n?', '', body)  # Removes commented lines from text
        sql = [i for i in post_body.split(';')]  # Splits string into list elements to be run consecutively
        for i in sql:
            if i != '\r\n':  # Reading a text file causes carriage return, removes text that isn't sql
                try:
                    print(i)
                    sf.query(i)
                    print('Completed!')
                    comp_counter += 1
                except:
                    user_input = input('FAILURE! Do you wish to continue? (y/n): ').lower()
                    if user_input == 'n':
                        sys.exit(1)
                    else:
                        fail_counter += 1
        print('{0} processing Complete! \n Checking for additional files...'.format(_file))
    sf.close()
    return print('Completed tasks: ', comp_counter, '\nFailed Tasks: ', fail_counter)


def read_through_bucket_directory(bucketname, directory):
    bucket_obj_list = read_bucket_objects(bucketname)
    directory_obj_list = [i for i in bucket_obj_list if i.startswith(directory) and i.endswith(".sql")]
    return directory_obj_list


def data_unzip_and_move():
    for i in read_directory('/home/brian/Downloads/'):
        orig_filename = os.path.basename(i)
        if orig_filename.startswith('On_Time_Reporting') and orig_filename.endswith('.zip'):
            while os.stat(i).st_size == 0:
                print('Waiting', end='\r')
                sys.stdout.flush()
            if '.csv' not in i:
                with zipfile.ZipFile(i, 'r') as zip_ref:
                    zip_ref.extractall(data_directory)
                for k in read_directory(data_directory):
                    if k.endswith('csv'):
                        full_new_file_path = os.path.join(data_directory, k[-10:])
                        os.rename(k, full_new_file_path)


def data_delete_cleanup(directory):
    for i in read_directory(directory):
        file_name = os.path.basename(i)
        file_date = file_name[-10:-3]
        if (file_name.startswith('On_Time_Reporting') and file_name.endswith('.zip')) \
                or (file_name.startswith(file_date)) \
                or (file_name.startswith('readme')):
            if file_date+'csv' in '\n'.join(read_bucket_objects(bucket_name)) or file_name.startswith('read'):
                os.remove(i)
    return None


# Main Script Execution
if __name__ == '__main__':
    web_crawler('July', '2019')
    data_unzip_and_move()
    upload_large_file(bucket_name)
    # sf = sf(user=username, password=password, account=account)
    # read_files_from_bucket_and_execute_sql(bucket_name, _dir)
    data_delete_cleanup('/home/brian/Downloads/')
    data_delete_cleanup(data_directory)