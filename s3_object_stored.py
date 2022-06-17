from typing import List
import boto3
import sys
from io import BytesIO
from datetime import datetime, timedelta
import json


aws_access_key_id='ASIAS34UIATUBME6MRNQ'
aws_secret_access_key='t7KAOqOJLOvqExfwkgp36CED8ZDSTlvSk6SErf0t'
aws_session_token='IQoJb3JpZ2luX2VjEMH//////////wEaCXNhLWVhc3QtMSJIMEYCIQCie3FjjeMJNCm/lUeXWUYwNyynsy9lLvvVcFYOK1/MxwIhAIWjXE5DO8pSj2SxEK3L48KbdcIZjg2tOGXjKkx9MnpCKrEDCMr//////////wEQARoMMTk3MzQyNTI4NzQ0IgyjBQgF1X3CqvArUJAqhQPtqwBb6ancOIvLf+hFzVV3wjed8F+GTdn0lROJ0sQvJzeenRdClF2a2zWXKyUX+xjv88eYRUj4ULOGtbSFX3VDKss/Ve7YBGir81q7X+c7Hn/SE7wDNUdd7qSyMjWfUCh+8TCWzoOkUoZP0nf/xM4IbbiuRicBaSM7EVV63p7WOVhO0qGNYQZzCPbIV0aX26BfeJP8778vvWW8DTdcEComSJhwLVAKyXJ8ARjvT1POUokSQ/W4SUGGgBF7DTEvkhrP0j5KInr48rosIl029JFOQRokD9gmqkZRTHuMAT3idCbpj+9Sj7ZpqgEeT41bZxsRhp+PoL31KZobmzHjWouWOAB7bhttXacuL0eZGCVlpabwyLF2ad5SsfsddjJpJ74q5B9UKhuTY2gGAYuHzN7gpfn7AiHkS2AGYd4iAQ0JgSJmLdGNbJdGMWW2lrdYSxtfzQ8+njYgYsWl02EZBRlp9DHR4cq09rPDL/LX8ALPbDbiS2gMRuKcMFJcJggTv545914l5zDeq6+VBjqlAeeEBg6+QOEiAJ/FqqqSH4roeY2v2sknLf5Df50CTowV7Lx5WAfysy0vbnhj0pjbIiHkR3s5HMWBiN93yi4FE4Nx8iabbbEB9y5JLfn1Y4z8OAOM3jXu//0rw6r/QD3Vcjyq8iuvOmr6i9ziuGOtl05Vng2nO6fDcz/0WuFFFEKXOw2uLjaF7ivoABsjoH74VF+b2fcVXiHTnH6VqfnBjRxKRA6Xkg=='
s3_client = boto3.client(
    "s3",
    aws_access_key_id = aws_access_key_id,
    aws_secret_access_key = aws_secret_access_key,
    aws_session_token = aws_session_token
    )

s3 = boto3.resource(
"s3",
aws_access_key_id = aws_access_key_id,
aws_secret_access_key = aws_secret_access_key,
aws_session_token = aws_session_token
)

bucket = s3.Bucket('captalys-analytics-trusted-production')
# for obj in bucket.objects.filter(Prefix='captalys/data/collector/v1/parquet/lake/postgres/geral/pricing_clj/master/pricing_clj_master_offer/'):
#     lst = obj.key.split('/')
#     parquet_name = lst[len(lst)-1]
#     path_store = ('/'.join(lst[:-1])) + '_store/' + parquet_name
#     print(path_store)
#     sys.exit()

def bkp_delete_files(bucket_name, prefix):
    s3 = boto3.resource(
         "s3",
    aws_access_key_id = aws_access_key_id,
    aws_secret_access_key = aws_secret_access_key,
    aws_session_token = aws_session_token
    )
    bucket = s3.Bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix=prefix):
        print(obj.key)
        lst = obj.key.split('/')
        parquet_name = lst[len(lst)-1]
        path_bkp_key = ('/'.join(lst[:-1])) + '_bkp/' + parquet_name
        s3.Object(bucket.name,path_bkp_key).copy_from(CopySource=(bucket.name + '/' + obj.key))
        # s3.Object(bucket.name,obj.key).delete()

bkp_delete_files('captalys-analytics-trusted-production','captalys/data/collector/v1/parquet/lake/postgres/geral/pricing_clj/master/pricing_clj_master_offer')
print('finalizado')