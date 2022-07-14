import boto3

def get_contents_by_prefix(s3_client, bucket_name, prefix):
    response = s3_client.list_objects_v2(
        Bucket=bucket_name,
        Prefix=prefix
    )
    
    if 'Contents' in response:
        return response['Contents']
    else:
        return []


if __name__ == "__main__":

    aws_access_key_id='ASIAS34UIATUK6U7M5VS'
    aws_secret_access_key='IzRH1MWGWjZsHvWVtOH32NH4WvkREfEN3dLcr8yJ'
    aws_session_token='IQoJb3JpZ2luX2VjEAEaCXNhLWVhc3QtMSJGMEQCIC9+6gz56mrpzGal13dup4kBN1wkosHa0+gZtDJn9kuQAiBat8Wlr9bfrU/vL8iPoPjJXSGhfYwvMlh5SVUEndwMHCqoAwg6EAEaDDE5NzM0MjUyODc0NCIM+qPnZsSbWqB1Rv9nKoUDKNWTu+kgwG5XIEy2ebT+xOKnOulp5C0ikZcY/6cTTwLUPlvyz8qtlAeAP4AsBXoaGPsSwndXsQqK79xAKWvTtHKu2gPh3CNx/QR29Iage+Az9PLUW+jevrrnrqUNg8Oil2+cr7QqRmFEBdFxqPaBUKparb5MueH4JoTE8jwWcVv9gfsFDcMflfmTFHKPDmDbc6qz4iOVieLM1i1yfrF+yFofoqUkLr/8dTjcy7phW2NaVGv4xCMWzqGy7Fp2ugOIQjQ1yEHwxeDSBGZCKle3In5dUecYM1CFdtwhZmbJ9EZhrpzoh2UFnXapLLbD2EsolrGqhAXvhGLEJvUaG9t7ToIEYS3BzSJox5VxHYbIBPgDWarWeuzLHokKn8TPfoKdnFgn/pp9ROctpIuumqOJ5TV2+mLmwDkS8Hu8PY5u8c87aA/PtvYYLP3mqMtKhQKVaR+zzuQDRBxMa6+VCdj2481TfJYQ+iiIYapQWbI1aFZEMBDp9w7iNuevR5yjp5B6/ALxyagwlt2tlgY6pwH/xUFJNjR1YYpKG6vemT8M6B8ROXANzLJaNgPWwZGl4+QvAym7g+g37Jz8/m4yP2OVO1FrSQu5vrFt1N/Rrg1XbCeKh7BT4xEUI5Rprp/v9drVBkJR9meftEbj2C0hQz7NHG18JVijg+1q9OJkLz2+2k4TOgNnoI/CLUCDvtis15KGYLh4JlH7/6CrU70tyhhXSl+gNV8dlKXYsnG6LOM5c/EcRw57Gg=='

    glue_client = boto3.client(
        "glue",
        aws_access_key_id = aws_access_key_id,
        aws_secret_access_key = aws_secret_access_key,
        aws_session_token = aws_session_token,
        region_name='sa-east-1'
        )

    s3_client = boto3.client(
        "s3",
        aws_access_key_id = aws_access_key_id,
        aws_secret_access_key = aws_secret_access_key,
        aws_session_token = aws_session_token
    )


    contents = get_contents_by_prefix(s3_client, 'captalys-analytics-land-production', 'captalys/data/generic/v1/zip/lake/dados_publicos/estabelecimentos/')

    list_keys = []
    for cc in contents:
        if cc['Size'] > 0:
            list_keys.append(cc['Key'])

    # sys.argv, ["bucket_name", "key_filename_zip", "path_filename_unzip", "extension"]

    list_dict_jobs_parms = []

    for lt in list_keys:
        obj = {
            '--key_filename_zip': lt,
            '--bucket_name': 'captalys-analytics-land-production',
            '--path_filename_unzip': 'captalys/data/generic/v1/csv/lake/dados_publicos/estabelecimentos/',
            '--extension': '.csv'
        }
        list_dict_jobs_parms.append(obj)
    
    for ldc in list_dict_jobs_parms:
        print(ldc)
        response = glue_client.start_job_run(
                JobName = 'datalake-unzip-file',
                Arguments = ldc )
    