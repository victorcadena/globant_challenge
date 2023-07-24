import json
import boto3
import base64
import psycopg2
from datetime import datetime
from botocore.exceptions import ClientError

def run(event, _):
    unprocessed_files = get_unprocessed_files(event)
    onnection = get_db_connection()
    # :
    #     pass

def process_file(file_path, db_connection: psycopg2.connection):
    with db_connection.cursor() as cursor:
        cursor.execute()


def get_unprocessed_files(event):
    try:
        datetime_object = datetime.now()
        client = boto3.client("s3")
        paginator = client.get_paginator('list_objects')
        operation_parameters = {'Bucket': event['source'],
                                'Prefix': f"{event['domain']}/{event['dataset']}/unprocessed"
                                }
        page_iterator = paginator.paginate(**operation_parameters)
        objects_list = []
        for page in page_iterator:
            if "Contents" in page:
                contents = page["Contents"]
                for dataset_object in contents:
                    objects_list.append(dataset_object["Key"])
            else:
                print("No objects found")
        return objects_list

    except Exception as e:
        print(e)
        raise e

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

def get_db_connection():
    database_secret = get_database_secret()
    connection = psycopg2.connect(
        database=database_secret['dbname'],
        user=database_secret['hr'],
        password=database_secret['password'],
        host=database_secret['host'],
        port=database_secret['port']
    )
    return connection