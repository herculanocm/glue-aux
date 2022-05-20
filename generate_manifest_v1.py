from typing import List
import boto3
import sys
from io import BytesIO
from datetime import datetime, timedelta
import json

aws_access_key_id='ASIAS34UIATUHWBXKL2P'
aws_secret_access_key='LERnBcUWkefDlevSrx1Ks/xKHMsFKJC2NkeCQRvu'
aws_session_token='IQoJb3JpZ2luX2VjEKr//////////wEaCXNhLWVhc3QtMSJGMEQCIFNH+jkod50jU9YWJZolwWSp0vH6UleD3PRHv/R0sXPLAiBLfHKlDmUVR7Upth1rDnh4uS6+KlFqhwB+o2jL+f3xXCqxAwiD//////////8BEAEaDDE5NzM0MjUyODc0NCIM2t7V9tRfqC3VOFKNKoUDaOEOUFFfoqYL+oVq+flolQT2qI5lcRwLuKjS0y0nsU6HHDlYcnfmGzs8C6rLB2IlBlEq2hLL8+JIw9Owoir/CRZxw/gDPB4A2nPe5Bl77kk+dbxrZBz7gZy5aaFgz0LdX+TSi9sfd1uzQmYjQteO8yLPxOo3C2juJ1gtIZSb6PI5Yu6E0cALN6mJoCJCdZU0uk0W2iJnawwDpOKs5L7K6Pe9uk5NY+cK2+AEBunz2JtN8TB34fNbwJU/YRT0GQ3jKw5qLN5rsmprKQCvtr72iVzQM8+9bfSVDeEl4Dl+CM52NtvaeILOx2OZyqupfEvpRUDQg1K5b5Q6KdZVJAEZ2yzhlPj5U6aqB0Dbjr97ha4UtvDY6Ftkl4O54UP5NPj3eJ3F80iV6765instPXjvFzkG6SCA6gqgOLvoR2LDSThXk0uRFBu37RxJVoSdVHi7xGjh/DxunBDFbvU+ym+lkDWhRHpF2d+QtEEP0w2hRBwoPBe2xcbD1swBeuxWVqGJAXVo5MswqayBlAY6pwFEfLzo3WIUjmwWlypt1OUCeSYAGSU6R1WXG7D7ujjpIz+UD2a3+tccsFMf7G7pTP4bR8gyqRyrOar0Wf/Wb7zmO62Y8qtfL8EYW1OfxwGz5rMoeMPw/pnVLUHIDRKO9+RLI9hyTSmmhYsU12fPx7DxO5Xew/EnFTBh4Aot6GFTKq7lpZcWgU6qHjiMdHjJv8p93nbweUP3Jx/ekx/SY690aON71ZUaCg=='

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

glue_args = {
    'bucket_name': 'captalys-analytics-land-production'
}

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


if __name__ == "__main__":
# comparar as publicações diárias
# captalys-analytics-land-production / captalys/ data/ collector/
    bucket_name = 'captalys-analytics-temporary'
    path_prefix = 'captalys/data/glue/v1/parquet/lake/'
    contents = get_contents_by_prefix(bucket_name, path_prefix)

    manifest_json = json.dumps(get_manifest_json(bucket_name, contents), indent=4)

    send_data_to_s3(bucket_name, (path_prefix + 'manifest.json') , manifest_json.encode('utf-8'))
    print('finalizado')
