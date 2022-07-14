# -*- coding: utf-8
from xmlrpc.client import Boolean
import boto3
import sys
from io import BytesIO, StringIO
import json
import urllib3
from botocore.exceptions import ClientError 
import pg8000 as pg
from datetime import datetime, timedelta
import pysftp 

import paramiko

aws_access_key_id='ASIAS34UIATUK5TRNN7D'
aws_secret_access_key='2zq4BI9KM7DdcNN6XX/NeHqV8VVK6t/32L9Kndhv'
aws_session_token='IQoJb3JpZ2luX2VjEC0aCXNhLWVhc3QtMSJHMEUCIBC89zE7GaQ5UFjOcY0emDS5MMQ2LiKeBUWWvsKpOfmrAiEAgQunyvnoacoEJYnk/xHVu4sguhqyGxOhLl773Ak2QpwqqAMIRhABGgwxOTczNDI1Mjg3NDQiDKb2h76Ujtl+/bbUoiqFA79q+TcVnI4wQmuyJiBUOrp/0y1JBXWxQGkLrhxgv0GCUTB/3ydhjXLv7Obx6kJBonDFOxE5ER6iuhEfjVM77mz5bs17FYjSU6eJnZudVZ415gHrHkjIbiVA6SIeU6YrsAcwT3ZEl2AyvqTw9te2xVLnIZ4JoQaqFn9w+7ey1oQ8dEAkiDh3kdV1mDsMcvkMMTNDuEnja0D7svcNlLDEntMkcBG9lgwhUWNtlqoicAqOXbNhNgm7WzJeBrjUNG6Hn4E9h+l32WGOZmTTYap8N66DaHR30VzTGTBoH/iPlim2azHmuw8ARFITYESsrgqR6Wn2OTm3AmfwFuwjFj0Uh8sXqeR4k2Bry1Hs1sfhq353l3tcK96H71PVxQ7yhRToiFvCWCI78ZwSOpU8mGhEGxmB/AVJej8YsCVggDBx9ypyfsiVDBJG0+glQieoH19AUslLNOg+XYCVU2uiY5xjwntoGtrRWXNG/Ra332LNaItU9IsMwZtUr3/ley13zNCA9/iJtDP9MPeAx5UGOqYBetYamVx0NH/nTP8/RJRlisx2Y3l/Xf56Rs0tZxosTZZyd2IbDZlc09twgNLErA+Utq+Du0FMMNIyfUYOoJsogz2Us9kjcIDYIiaaYqzVE5eK6KRcCOswA9nLpDuyKL5FhgdjkqRm4QNnDEgbWxAl+hVWnQpIYi06B+P+JPykb0shDz/8/g8m7OxdaEx59Huspz/x7gfZXjt7C8qVYiSkhtUT41fNpw=='
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
    "bucket_name": 'captalys-analytics-land-production', 
    "port": '22',
    "host": 'sftran.brltrust.com.br',
    "password": 'w@ferreira5',
    "username": 'captalys'
}

def get_text_by_key(bucket_name, key):
    result = s3_client.get_object(Bucket = bucket_name, Key = key)
    text = result["Body"].read().decode()
    return text

def get_connection(connection_name):
    response = glue_client.get_connection(Name=connection_name)
    
    # https://docs.aws.amazon.com/glue/latest/dg/connection-defining.html#connection-properties-jdbc
    jdbc_url = response['Connection']['ConnectionProperties']['JDBC_CONNECTION_URL'].split('/')
    database_port = jdbc_url[2].split(':')
    
    conn_param = {}
    conn_param['database'] = jdbc_url[3]
    conn_param['hostname'] = database_port[0]
    conn_param['port'] = database_port[1]
    conn_param['user'] = response['Connection']['ConnectionProperties']['USERNAME']
    conn_param['passsword'] = response['Connection']['ConnectionProperties']['PASSWORD']
    
    return conn_param

def send_data_to_s3(bucket_name, data_bytes, key_file_name) -> None:
    s3_file = BytesIO(data_bytes)

    s3_client.put_object(
        Body=s3_file.getvalue(),
        Bucket=bucket_name,
        Key=key_file_name
    )


class SftpConnector:
    def __init__(self, hostname=None, port=None, username=None, private_key=None) -> None:
        self.hostname = hostname
        self.port = port
        self.__username = username
        self.__private_key = private_key
        self.sftp_brl = None
        self.sftp_vortex = None

    def vortx_conection(self):
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None

        self.sftp_vortex = pysftp.Connection(
            host=self.hostname,
            username=self.__username,
            private_key=self.__private_key,
            port=self.port,
            cnopts=cnopts
        )
        print("Connection succesfully SFTP_Vortex ... ")
        return self.sftp_vortex

    def brl_conection(self, password=None):
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None

        self.sftp_brl = pysftp.Connection(
            host=self.hostname,
            username=self.__username,
            private_key_pass=password,
            private_key=self.__private_key,
            port=self.port,
            cnopts=cnopts
        )
        print("Connection succesfully SFTP_BRL... ")
        return self.sftp_brl


class ExtractSftpBrl:

    def __init__(self, hostname=None, port=None, username=None, private_key=None, password=None):
        self.__conn = SftpConnector(hostname=hostname, port=port, username=username, private_key=private_key)
        self.__password = password
        self.brl = self.__conn.brl_conection(self.__password)
        self.s3_files_data = {}

    def brl_list_path_files(self, adm: str, path: str, day: str, prefix: str):
        path_s3_brl_files_prefix = f'{adm}/data/sftp/v1/zip/generic/'
        period = day.split("-")
        s3_uri = []
        name_files = []

        with self.brl.cd(path):
            directory_structure = self.brl.listdir_attr()
            print(f'Arquivos no path: {adm.lower()}{self.brl.pwd}')

            # Lista os arquivos no diretorio
            for attr in directory_structure:
                try:
                    if (day in attr.filename) and (prefix.upper() in attr.filename):

                        name_files.append(attr.filename)
                        s3_uri.append(
                            f'{path_s3_brl_files_prefix}{prefix}/'
                            f'{period[0]}/{period[1]}/{period[2]}'
                            f'/{attr.filename}')

                        self.s3_files_data['key_name'] = name_files
                        self.s3_files_data['s3_uri'] = s3_uri
                        # print(self.s3_files_data)

                except FileNotFoundError as e:
                    print('Erro: Arquivos não encontrados com os parametros informados ', e)

            # download dos Arquivos
            if len(self.s3_files_data) >= 1:
                for file in self.s3_files_data['key_name']:
                    flo = BytesIO()
                    numbytes = self.brl.getfo(file, flo)
                    flo.seek(0)
                    databytes = [byt for byt in flo]
                    self.s3_files_data['databytes'] = databytes

            else:
                return(f'Arquivos do dia: {day}, vazios')



#private_key = "s3://captalys-analytics-land-production/brl/Key/sftp/v1/key-pem/acessoBRL.pem"
# public_key2 = s3_client.getvalue("s3://captalys-analytics-land-production/brl/Key/sftp/v1/key-pem/acessoBRL.pem")

if __name__ == "__main__":
    
    str_key = get_text_by_key('captalys-analytics-land-production', 'brl/Key/sftp/v1/key-pem/acessoBRL.pem')
    private_key_file = StringIO()
    private_key_file.write(str_key)
    private_key_file.seek(0)
    private_key = paramiko.RSAKey.from_private_key(private_key_file, password=glue_args['password'])
    brl = ExtractSftpBrl(hostname=glue_args['host'], port=22, username=glue_args['username'], private_key=private_key, password=glue_args['password'])

    # Parametros arquivos
    brl.brl_list_path_files('brl', 'estoque_diario', '2022-06-20', 'estoquediario')
    

    
    # Salvando arquivos
    # databytes = brl.s3_files_data.get('databytes')[0]
    # for s3_path in brl.s3_files_data.get('s3_uri'):
    #     send_data_to_s3(glue_args['bucket_name'], databytes, s3_path)