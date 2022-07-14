import boto3

lista_urls_empresas = [
    'http://200.152.38.155/CNPJ/K3241.K03200Y1.D20611.EMPRECSV.zip',
    'http://200.152.38.155/CNPJ/K3241.K03200Y2.D20611.EMPRECSV.zip',
    'http://200.152.38.155/CNPJ/K3241.K03200Y3.D20611.EMPRECSV.zip',
    'http://200.152.38.155/CNPJ/K3241.K03200Y4.D20611.EMPRECSV.zip',
    'http://200.152.38.155/CNPJ/K3241.K03200Y5.D20611.EMPRECSV.zip',
    'http://200.152.38.155/CNPJ/K3241.K03200Y6.D20611.EMPRECSV.zip',
    'http://200.152.38.155/CNPJ/K3241.K03200Y7.D20611.EMPRECSV.zip',
    'http://200.152.38.155/CNPJ/K3241.K03200Y8.D20611.EMPRECSV.zip',
    'http://200.152.38.155/CNPJ/K3241.K03200Y9.D20611.EMPRECSV.zip',
]

lista_urls_estabelecimentos = [
    'http://200.152.38.155/CNPJ/K3241.K03200Y0.D20611.ESTABELE.zip',
    'http://200.152.38.155/CNPJ/K3241.K03200Y1.D20611.ESTABELE.zip',
    'http://200.152.38.155/CNPJ/K3241.K03200Y2.D20611.ESTABELE.zip',
    'http://200.152.38.155/CNPJ/K3241.K03200Y3.D20611.ESTABELE.zip',
    'http://200.152.38.155/CNPJ/K3241.K03200Y4.D20611.ESTABELE.zip',
    'http://200.152.38.155/CNPJ/K3241.K03200Y5.D20611.ESTABELE.zip',
    'http://200.152.38.155/CNPJ/K3241.K03200Y6.D20611.ESTABELE.zip',
    'http://200.152.38.155/CNPJ/K3241.K03200Y7.D20611.ESTABELE.zip',
    'http://200.152.38.155/CNPJ/K3241.K03200Y8.D20611.ESTABELE.zip',
    'http://200.152.38.155/CNPJ/K3241.K03200Y9.D20611.ESTABELE.zip'
]

lista_urls_socios = [
    'http://200.152.38.155/CNPJ/K3241.K03200Y0.D20611.SOCIOCSV.zip',
    'http://200.152.38.155/CNPJ/K3241.K03200Y1.D20611.SOCIOCSV.zip',
    'http://200.152.38.155/CNPJ/K3241.K03200Y2.D20611.SOCIOCSV.zip',
    'http://200.152.38.155/CNPJ/K3241.K03200Y3.D20611.SOCIOCSV.zip',
    'http://200.152.38.155/CNPJ/K3241.K03200Y4.D20611.SOCIOCSV.zip',
    'http://200.152.38.155/CNPJ/K3241.K03200Y5.D20611.SOCIOCSV.zip',
    'http://200.152.38.155/CNPJ/K3241.K03200Y6.D20611.SOCIOCSV.zip',
    'http://200.152.38.155/CNPJ/K3241.K03200Y7.D20611.SOCIOCSV.zip',
    'http://200.152.38.155/CNPJ/K3241.K03200Y8.D20611.SOCIOCSV.zip',
    'http://200.152.38.155/CNPJ/K3241.K03200Y9.D20611.SOCIOCSV.zip'
]

list_dict_empresas = []
list_dict_estabelecimentos = []
list_dict_socios = []

for ll in lista_urls_empresas:
    obj = {
        '--bucket_name':   'captalys-analytics-land-production',
        '--url_download': ll,
        '--destination_key': ('captalys/data/generic/v1/zip/lake/dados_publicos/empresas/' + ll.split('/')[-1])
    }
    list_dict_empresas.append(obj)

for ll in lista_urls_estabelecimentos:
    obj = {
        '--bucket_name':   'captalys-analytics-land-production',
        '--url_download': ll,
        '--destination_key': ('captalys/data/generic/v1/zip/lake/dados_publicos/estabelecimentos/' + ll.split('/')[-1])
    }
    list_dict_estabelecimentos.append(obj)

for ll in lista_urls_socios:
    obj = {
        '--bucket_name':   'captalys-analytics-land-production',
        '--url_download': ll,
        '--destination_key': ('captalys/data/generic/v1/zip/lake/dados_publicos/socios/' + ll.split('/')[-1])
    }
    list_dict_socios.append(obj)

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

    print('executando jobs')
    for lk in list_dict_socios:
        print(lk)
        response = glue_client.start_job_run(
                JobName = 'dados-publicos-download',
                Arguments = lk )