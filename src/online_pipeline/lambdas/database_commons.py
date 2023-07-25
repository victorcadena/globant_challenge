import json
import boto3
import base64
import psycopg2
from datetime import datetime
from botocore.exceptions import ClientError
from psycopg2.extras import RealDictCursor
import os

def _get_database_secret(event=None):
    
    if not event:
        target_db_secret_arn = os.getenv("TARGET_DB_SECRET")
    else:
        target_db_secret_arn = event.get("target_db_secret", None)
    
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

def get_db_connection(event=None):
    database_secret = _get_database_secret(event)
    connection = psycopg2.connect(
        database=database_secret['dbname'],
        user=database_secret['username'],
        password=database_secret['password'],
        host=database_secret['host'],
        port=database_secret['port']
    )
    return connection

def get_json_results_from_db(sql, parameters = None, connection = None):
    if not connection:
        connection = get_db_connection()
    result = []
    try:
        with connection:
            with connection.cursor(cursor_factory=RealDictCursor) as cur:
                if parameters:
                    cur.execute(sql, parameters)
                else:
                    cur.execute(sql)
                result = cur.fetchall()
        return result
    except Exception as e:
        raise e
    finally:
        connection.close()
