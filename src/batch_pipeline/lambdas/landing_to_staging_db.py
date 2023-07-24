import json
import boto3
import base64
import psycopg2
from datetime import datetime
from botocore.exceptions import ClientError

def run(event, _):
    last_processed_timestamp = get_last_processed()
    connection = get_db_connection()
    with connection.cursor() as cursor:
        pass

def get_matching_files(event, last_processed_timestamp):
    datasrt_url = f"{event['source']}/{event['dataset']}"
    datetime_object = datetime.strptime(last_processed_timestamp, '%Y-%m-%d %H:%M:%S')

def get_last_processed():
    client = boto3.client('dynamodb')
    data = client.get_item(
        TableName='processed_tracker',
        Key={
            'pipeline_id': {
                'S': 'landing_to_staging'
            }
        }
    )
    print(json.dumps(data))
    return data

def get_database_secret(event):
    target_db_secret_arn = event.get("target_db_secret")
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name='us-west-2'
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=target_db_secret_arn
        )
    except ClientError as e:
        raise e
    else:
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            secret = base64.b64decode(get_secret_value_response['SecretBinary'])
    
    database_secret = json.loads(secret)
    return database_secret

def get_db_connection() -> psycopg2.connection:
    database_secret = get_database_secret()
    connection = psycopg2.connect(
        database=database_secret['dbname'],
        user=database_secret['hr'],
        password=database_secret['password'],
        host=database_secret['host'],
        port=database_secret['port']
    )
    return connection