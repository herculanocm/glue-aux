from typing import List
import boto3
import sys
from io import BytesIO
from datetime import datetime, timedelta
import json


aws_access_key_id='ASIAS34UIATUF342XHGS'
aws_secret_access_key='BM3xDtY4PJw1POsUdzHWokDlR14oGRR8j4W5qrpC'
aws_session_token='IQoJb3JpZ2luX2VjEOv//////////wEaCXNhLWVhc3QtMSJHMEUCIQCDIvCp8B90e0atlug3iljahqbBkGs6QfYjZUbo8PLvDAIgQHioFtFMfYjnG1eQx5KwzoWZbmYgNCqUqcQcrb8sGhsqsQMIxP//////////ARABGgwxOTczNDI1Mjg3NDQiDO/CwMIjvgJarA3xDiqFA6kvqQpkqBPmI8iNs2I1LLxwPdh2MC6T4fPaZ8qs7O4g6wOh3OWE9tH6sHe9yn6JZhCPL6X0DLQQ5Gry8be11p1IGaW8tjbJlBQyhPupBvbie6+icr6sb3wvi3MQ/rP7EbaJFrlPk6jpBmf8ulX0V9GWdmhg6ROcHkl0n9ZpzLeVoq+hZYfNW3SpoCS2G3SxxZvZoUwQjlwGvElKvQJ3DlkbGAd1vahv0iJHfzFckmJnUy7lZja7o9LvVEmCDcVGrxDxHi8mZi7E9e6tzxNrv+A8Z/fG+WU878qEr/RUBUM/+NlYCILYxCAlk3KPu4+XJexwat9Cl7aaHLYESA97YXHZ/3lVKZfCbINIbSP+Bo5vsaJeepbbYwCeB6cj0AcxF7fea2KJ+OKJ8Q6h2juaO/bPdTwkDWYQQ4jzQ0o/nNiktWKbcR1IHtAIJ3bU0P/K0O5c+h2WDJ7WIxezPj2Mk9gmbVoqHBhH0KJH5ZZN7oqUu5bdH2wVd4i0bBtWZMWxYcACiy4TMOXmj5QGOqYBkAxnJ2qh9hVrMRX32l8ycqfLTG4HmDeVKyusrJdxv6YrIUB0JzplxuKlfD9hixHW0Jqy895FOF/xNmYxxvSi7C9DhSzlbhZAAg96iawUet5ubiy5xHjQ7wzuAkqCxG55ioq6tdjGDE+96FnWZ0YhPcVdpB/FONablfdVxIPQhpRKHztCodP++lkxe+nc9Ggj435p8LujWMTmjnez+WztzilVa3eISw=='


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
