from app.modules.snowflake_connection_class import sf
from app.modules.s3_class import s3
from app.modules.secrets_manager import get_secret
import re
import os

from dotenv import load_dotenv

load_dotenv()
account_id = os.getenv('account_id')
secret = os.getenv('secret')
bucket = os.getenv('bucket_name')
database = os.getenv('database')

secret_dict = get_secret('brian-snowflake-secret', account_id, secret)
sf_user = secret_dict['user']
sf_pass = secret_dict['password']
sf_account = secret_dict['account']
sf = sf(sf_user, sf_pass, sf_account)
s3 = s3(account_id, secret)


def read_load_files_from_bucket_and_execute_sql(bucketname, directory, data_directory, _id, _secret):
    dict_counter = {'fail_counter': 0, 'comp_counter': 0}
    for _file in s3.read_through_bucket_directory(bucketname, directory, 'sql'):
        if 'Load' in _file:
            obj = s3.s3_resource(_id, _secret).Object(bucketname, _file)
            body = obj.get()['Body'].read().decode('utf-8')  # Reads the s3 object and returns a string
            post_body = re.sub(r'(?m)^ *--.*\n?', '', body)  # Removes commented lines from text
            sql = [i for i in post_body.split(';') if i != '\r\n']
            # Splits string into list elements to be run consecutively but doesnt include return carriage
            sf.query('USE DATABASE {0}'.format(database))
            for i in sql:
                try:
                    # Run sql queries from text file
                    sf.query(i)
                    dict_counter['comp_counter'] += 1
                except:
                    dict_counter['fail_counter'] += 1
            for data_file in s3.read_through_bucket_directory(bucketname, data_directory, 'csv'):
                # Iterate through each csv file
                data_file_name = os.path.basename(data_file)
                # Insert current file iteration into sql statement to check if load was successfule in snowflake
                like_sql_file = '%'+data_file_name+'%'
                df = sf.query("SELECT FILE_NAME, STATUS, ROW_COUNT, ROW_PARSED FROM INFORMATION_SCHEMA.LOAD_HISTORY "
                              "WHERE FILE_NAME LIKE '{0}'".format(like_sql_file))
                file_name = str(df['FILE_NAME'].values).replace('\'', '').replace(']', '').replace('[', '')
                status = str(df['STATUS'].values).replace('\'', '').replace(']', '').replace('[', '')
                if (str(data_file) in file_name) and (status == 'LOADED'):
                    # Move data file to archive folder if data was successfully inserted
                    s3.move_bucket_object(bucketname, data_file, os.path.join('FAA/Archive', data_file_name))

    sf.close()


def lambda_handler(event, context):
    read_load_files_from_bucket_and_execute_sql(bucket, 'FAA/Scripts', 'FAA/Source Data', account_id, secret)
    return {
        'statusCode': 200,
        'body': 'success'
    }
