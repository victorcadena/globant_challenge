import json
import boto3
import base64
import psycopg2
from datetime import datetime
from database_commons import get_db_connection
from botocore.exceptions import ClientError

def run(event, _):
    connection = None
    try:
        unprocessed_files = get_unprocessed_files(event)
        connection = get_db_connection(event)
        for unprocessed_file_path in unprocessed_files:
            process_file(event, unprocessed_file_path, connection)
            move_file_to_processed(bucket=event['source'], file_path=unprocessed_file_path)
        
    except Exception as e:
        raise e
    finally:
        if connection:
            connection.close()

def process_file(event, file_path, db_connection):
    key_id, secret_key = get_s3_import_credentials(event)
    import_query = f"""
    select aws_s3.table_import_from_s3 (
        'staging_{event['dataset']}', 
        '',
        '(format csv, header false)',
        '{event['source']}', 
        '{file_path}', 
        'us-west-2',
        '{key_id}',
        '{secret_key}'
    )
    """
    with db_connection.cursor() as cursor:
        print(import_query)
        cursor.execute(import_query)
    db_connection.commit()

def get_unprocessed_files(event):
    try:
        
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
                    if ".csv" in dataset_object["Key"]:
                        objects_list.append(dataset_object["Key"])
            else:
                print("No objects found")
        print(objects_list)
        return objects_list

    except Exception as e:
        print(e)
        raise e

def get_s3_import_credentials(event):
    rds_secret_arn = event.get("rds_secret_arn")
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name='us-west-2'
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=rds_secret_arn
        )
    except ClientError as e:
        raise e
    else:
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            secret = base64.b64decode(get_secret_value_response['SecretBinary'])
    
    credentials = json.loads(secret)
    return credentials["aws_access_key_id"], credentials["aws_secret_access_key"]


def move_file_to_processed(bucket, file_path):
    client = boto3.client('s3')
    response = client.list_objects_v2(Bucket=bucket, Prefix=file_path)
    source_key = response["Contents"][0]["Key"]
    copy_source = {'Bucket': bucket, 'Key': source_key}

    processed_file_path = file_path.replace("unprocessed", "processed")
    print(f"moving file from {file_path} to {processed_file_path}")
    client.copy_object(Bucket = bucket, CopySource = copy_source, Key = processed_file_path)
    client.delete_object(Bucket = bucket, Key = source_key)