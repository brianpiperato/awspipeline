import boto3
import ast


def get_secret(secret_name, _id, _secret):
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        aws_access_key_id=_id,
        aws_secret_access_key=_secret,
        service_name='secretsmanager',
        region_name=region_name
    )
    get_secret_value_response = client.get_secret_value(
        SecretId=secret_name
    )
    _dict = ast.literal_eval(get_secret_value_response['SecretString'])

    return _dict



