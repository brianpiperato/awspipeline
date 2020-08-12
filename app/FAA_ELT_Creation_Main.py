# Purpose: AWS data etl main script to execute etl functions from various modules
# Created by: Brian Piperato
# Created on: 10/2/2019
# Last Modified on: 10/30/2019

import re
from dotenv import load_dotenv
from app.modules.file_manipulation import *
from app.modules.s3_class import *
from app.modules.snowflake_connection_class import sf

load_dotenv()
username = os.getenv('username')
password = os.getenv('password')
account = os.getenv('account')
account_id = os.getenv('account_id')
secret = os.getenv('secret')
bucket_name = 'abh-faa-stage-bp'
data_directory = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data/')


def read_files_from_bucket_and_execute_sql(bucketname, directory, _id, _secret):
    comp_counter = 0
    fail_counter = 0
    for _file in s3.read_through_bucket_directory(bucketname, directory, 'sql'):
        obj = s3.s3_resource(_id, _secret).Object(bucketname, _file)
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
                except BotoCoreError as e:
                    logging.error(e)
                    user_input = input('FAILURE! Do you wish to continue? (y/n): ').lower()
                    if user_input == 'n':
                        sys.exit(1)
                    else:
                        fail_counter += 1
        print('{0} processing Complete! \n Checking for additional files...'.format(_file))
    sf.close()
    return print('Completed tasks: ', comp_counter, '\nFailed Tasks: ', fail_counter)


def data_delete_cleanup(bucketname, directory):
    # Deletes files in directory for pipeline to keep memory usage at acceptable levels
    for i in read_directory(directory):
        file_name = os.path.basename(i)
        file_date = file_name[-10:-3]
        if (file_name.startswith('On_Time_Reporting') and file_name.endswith('.zip')) \
                or (file_name.startswith(file_date)) \
                or (file_name.startswith('readme')):
            # If a file startswith defined initial file name and ends with zip extension or the new file name, delete
            if file_date+'csv' in '\n'.join(s3.read_bucket_objects(bucketname)) or file_name.startswith('read'):
                os.remove(i)
    return None


# Main Script Execution
if __name__ == '__main__':
    s3 = s3(account_id, secret)
    sf = sf(user=username, password=password, account=account)
    read_files_from_bucket_and_execute_sql(bucket_name, 'FAA/Scripts', account_id, secret)

