from typing import List
import boto3
import sys
from io import BytesIO
from datetime import datetime, timedelta
import json


aws_access_key_id='ASIAS34UIATUcccGSGZ57YE'
aws_secret_access_key='AtQKE3Pdw7qIsccczwCDIsOToFOrLIDhIjTHKCWwv02'
aws_session_token='IQoJb3JpZ2cccluX2VjEKP//////////wEaCXNhLWVhc3QtMSJHMEUCIQCIM9hsXh8Ggiem80fsbbfkewya0fiaO+awxcufmPWu0AIgaWLyrow7b7Q65x4W3vI7AxfaOvZk/BJf8s+xAE54bQQqsQMIrP//////////ARABGgwxOTczNDI1Mjg3NDQiDDQV+iQVWvfvXJeMdiqFA1Og1jSu/UYOEcGeOtC/HsADvLkHhJINKLp3kqNLJKCSsfPMnachDAfaPcoryAMi1C9j+6cBARQR99UBHJImUbIeemELPBpEK/4IN2O3mHRU2EEISzo0bXhVDHwpS94RIcLfm9T77AFUQ2dZ1LG/NxQnRL6EeyCKhJLITz70ljKDhTFLlyBPl4jSflcpUncjHw7bOho0tncBHJUZRpUMVL5/G7On7aGFaZ0iz5lUpfcdhNzB6RD4aeSQ0VAqa0EDjJckpbh4sUDxn9EGy+dO4D6y6ciZyVJ8vNo4758SzKXOCXf7ekT5kxu4pLU3zN5dRS1LMtDTdCEN7s50t40Y5Wp3KfXCM9Erftju9p0aRPoUkeUogei6vAcPDT4oVqyS6leTc0tByaFcdKZgjdRA4k2p48TdPtVh/oJdROMl/KmfgmyeiD4EIMMj1oFm7T+dh9toYmsLceIKmgWhzxRLbWzFZgH6VFPMQquG3ILFKF8kXWzS3yIixFI94/5b/UPfcn75JxS8MOHZqJUGOqYB9wGMzh/gic6WbyWqUkDGbEpEWfAtumjR31DxKZfBbztYypvHiMmnvHQE/fUZ7Vb2OhkihCVB5RMPy5JI1enHTv1XLDmsBFj+E0NxxtLIpZ9LGJbqSq93IdgegHKze/5DsQYerMQcDu3QUyLzB76ltguVs3k2pJUqV9c6TGWS9VJWwnSuN10u43RNOR97gIZJ19/lEx8rAC5S23BAiLASKeg1HwCrCA=='

s3_client = boto3.client(
    "s3",
    aws_access_key_id = aws_access_key_id,
    aws_secret_access_key = aws_secret_access_key,
    aws_session_token = aws_session_token
    )

glue_client = boto3.client(
    "glue",
    aws_access_key_id = aws_access_key_id,
    aws_secret_access_key = aws_secret_access_key,
    aws_session_token = aws_session_token,
    region_name='sa-east-1'
    )



def get_contents_by_prefix(bucket_name: str, key_prefix: str):
    response = s3_client.list_objects_v2(
        Bucket=bucket_name,
        Prefix=key_prefix
    )
    
    if 'Contents' in response and isinstance(response['Contents'], list) and len(response['Contents']) > 0:
        return response['Contents']
    else:
        return []

def get_manifest_json(bucket_name: str, contents: List):
    manifest = {"entries": []}
    for fl in contents:
        if 'Size' in fl and fl['Size'] > 0:
            manifest_row = {"url": ('s3://' + bucket_name + '/' + fl['Key']), "mandatory": True}
            manifest['entries'].append(manifest_row)
    return manifest

def send_data_to_s3(bucket_name: str, key_file_name: str, data_bytes) -> None:
    s3_file = BytesIO(data_bytes)

    s3_client.put_object(
        Body = s3_file.getvalue(),
        Bucket = bucket_name,
        Key = key_file_name
    )

def get_table_schema(args, client):
    schema_info = {}
    schema_info['col'] = []
    schema_info['pk'] = []
    
    bucket = args["land_bucket"]
    file = f'{get_file_key(args, "schema")}.json'
    print(f"file : {file}")
    result = client.get_object(Bucket=bucket, Key=file) 
    text = result["Body"].read().decode()
    schema = json.loads(text)
    
    for f in schema:
        schema_info['col'].append(f['column_name'])
        

        if f['primary_key']:
            schema_info['pk'].append(f['column_name'])
                
    schema_info['col'].append('sys_commit_time')
    schema_info['col'].append('sys_file_date')
    schema_info['col'].append('sys_file_name')
    if args["domain"] == 'recebiveis':
        schema_info['col'].append('sys_operation')
        schema_info['col'].append('sys_commit_timestamp')
        schema_info['col'].append('sys_transaction_id')
                
    return schema_info

def get_file_key(glue_args, conf):
    path_key = f"{glue_args['domain']}/{conf}/{glue_args['tecnologia']}/v1/json/lake/{glue_args['sgbd']}/{glue_args['instancia']}/{glue_args['database']}/{glue_args['schema']}/{glue_args['table']}"
    print(f"path_key: {path_key}")
    return path_key

if __name__ == "__main__":
    glue_args = {
        'date': '2022-05-15', 
        'source_bucket': 'captalys-analytics-raw-production',
        'land_bucket': 'captalys-analytics-land-production',

         
         'domain': 'cci',
         'tecnologia': 'dms',
         'sgbd': 'postgres',
         'instancia': 'geral',
         'database': 'sci',
         'schema': 'public',
         'table': 'atendimento',
         'full_load': '1'
    }

    table_schema = get_table_schema(glue_args, s3_client)

    print(table_schema)
