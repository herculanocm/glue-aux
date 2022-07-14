
import pysftp
from io import StringIO
import paramiko


import json
import boto3


aws_access_key_id='ASIAS34UIATUOK7U3X72'
aws_secret_access_key='Rm/jDG5jlgll/knxkbHeNl4hYI3YETAJk/prnK6Z'
aws_session_token='IQoJb3JpZ2luX2VjEBwaCXNhLWVhc3QtMSJIMEYCIQC/0LyovrhYAd/6ZyHXJCl3J8MdDrgnVS6m6jHZmT3BUgIhAM+U94ZxIpE/MZJFQDc5qtLQnxdk1lKw6XKP7eT8BJc5KqgDCDUQARoMMTk3MzQyNTI4NzQ0IgyizzHREaql6EcUL14qhQOf7t/YmQmBrS+lnu+glTNTQBO9yuetIZLYg14FciGr8N0WPTxSqRJhhCzRzAr/5xVaBJMpMjIUKj9J/KDc/31aD/8Us1F3U4I6AsqLhjrqZxEJXwj/l0oTk6uDOGCop4BQ/c1pIVO+lYrdbfUEQhwdw6wgBspIpbKg8yAH1ULlem5Nhxqk0LtQ8tpIl/ewIAHAlWBcMbWNkE479ITl9pr/tbCHY3bbq5vcNRf9UgPq90E96Ayz3oXggd+wHSV56BR1dMe68s1bavAuxJuDdp0Lw0CKfzq99UyZksJroasFNT3kbQ9577iu3oontT2oOtRe3wSIo69rO0RwgJ+SJeE6EZmXGOTgijw9TLuuoRihDktLBktli6deVB6aQ8xpTm7tKBVp0rfo2E0c1OJ+V6VUbrACv+PHWByh042c0sTbFeYC5mnjUDPX5VcaLX5LsXju83HCw2YDA/0Wlf2wwlCRYkrtevo6YofsqeIrox/gLpwywZ3oHzZDleXoXWG1d9RigNOFejDvpcOVBjqlAZiC0Db5FDEhh0pyq7bQOC75F9p4cbXfHNCNI/1XOJvLCUqF50ZbpfM1aO/eHG9w/UfZPf7S3S+IfY5xhaacN6GtEXM4xoHMA1i9NmnpfJ+qtCMkWNVjrH7YHooMTPjfbXo0ydTIntda4ihBuPdwCIDYcpkW6q29MCX7oPsVrIzzlWvyT5+AT23+5mV9J0F49fnzAGTQlgESDjEOZpRaOcih6YTGlA=='
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


def get_text_by_key(bucket_name, key):
    result = s3_client.get_object(Bucket = bucket_name, Key = key)
    text = result["Body"].read().decode()
    return text



class SftpConector:
    def __init__(self, hostname=None, port=None, username=None, password=None, private_key=None) -> None:
        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
        self.private_key = private_key

    def open_conection(self):
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None

        with pysftp.Connection(
                host=self.hostname,
                username=self.username,
                private_key=self.private_key,
                private_key_pass="w@ferreira5",
                port=22,
                cnopts=cnopts
        ) as sftp:
            print("Connection succesfully stablished ... ")

            directory_structure = sftp.listdir_attr()
            for attr in directory_structure:
                print(attr.filename, attr)


brl_host = 'sftran.brltrust.com.br'
brl_username = 'captalys'
brl_private_key = '/home/hc/dev/sftp/acessoBRL.pem'
brl_password = "w@ferreira5"
port = 22

str_key2="""
-----BEGIN RSA PRIVATE KEY-----
Proc-Type: 4,ENCRYPTED
DEK-Info: DES-EDE3-CBC,F7F728DC188F9401

voVL8XwWmEO5EHb32Fy1rytKT2FO+rlNUubiYtFPwCQXlYgvv89EmYDBeI5aZ1rv
GN9w1MX7WHPzsk1tlh+oRHtAUZ74AMWMKN3Ll3dxi/+F4bAjsbSRtZYZjA2AZ2xa
9rv3S8paCKStW5OPsCPcdDQc6pesqMYZspp4H94zyte0pFMZQv83LR8U3tVx/wj6
SIW0utHhIcPMj9xj3r7IebTf3n3ywN8OeWtah36jEK+KmnuMcGQWK1+lrC/QdUJL
DXH9yZA3B3izSrYRG6rfsfMS/uPk+UXyqcQzTQn1/xpKe56wuX+UiruNfZp+JP55
HXwp6jmy5h5iau9n7iPWvYpQmQ5veHnCeSANINBGQfKnfg7BFcWePzkC7cZLjqB8
aN+ZoxMz8Oz3jO3XAmZssm/V8WZZRtFYC0WEdMfLHbDUShEL3VjnmdGDWilwnf2V
ydLtBZyYvRxb9l7EVtAymPlG4D3VIJ13CmZ1DFkJLHqGwAoFRsemEXcIfNWKEvKZ
0ZQqVO+VgJis+g8utaeC4W34JwJFymEvXRBsqPl6YCjvd/3DUTjLA/kvD/3Ie8Kb
LukbYF3HXKDlfknA+ozBUl3+CACZvxG2kJRFHa1ddhebQGDhRaulJPHcdYijKWCM
JkaIpCjSYSWNd226j+U8u9gTXRezzCQ7N6CD9NQe6ZHSIsmM5aptxV0+9Wfp3VEm
yfzonYj8uKDDtNTbqrPTFVuTuPDBxjIhHAyo7KvV+BYZeLkSIgjSmOhuO35v4wGj
BkcwtQsPX6gkbkalHK6rYSyRk9C5xUqfnLLxwzYixF3bKWzHsiBLY9YSRzRIEmuk
dSEpujWDKBbjefE9ns298DmYxz+V3bpDll2t+oOno8jwUesSlD6UYI/c94LBIwiV
JI9fMiLeWYmUccpwleKUiR637coOV199mTtgCl63PmWTECpe7h4nKMva2uRX03KB
wpK6HgALhWN+3Fnc0xZ1X1yAHswTA5ReDO6oBU6wSTE4Krijs3hMmpeMQf9gFy/S
5ZQAzWSoTOwdE8OX6kGn1zeC8/o0OA0cqUSySARGB9TXU95m+50yoJwtNQ19n+77
CmvyfIsEcTENA5VRfzgnIs3TKLnD5EKXOKDGKjcwiCPKRAngvEmVdMCnTlPgUwib
/ebbB3yOK93RyA8LbzFa7fUv5ZrAk6PS5ASc40ZzIDKcfVoSOAzRa71/vKIyd7y0
I/wIY0ggUW90j6dCSVF5JXf73lUr6pUcp1nKeV1d+fcU6w00dWN1J8HLdozi5VIi
JZg5PG6EjfXufGUIWa3JTDrQV5Wrn2QyKBebDdRlSwY/upMDlGHG0Va3vCW1c6V3
Qvc/TFD6o7Tnf09O9KSCstjIQpcsIiDe8uiA63Gv6zD0T8YuFG0ZMMG+KmcVg1d5
E4Ib3tWdcbGpKqfml27kKb9OMOMK8/XHSyDAEvz5DcF9IXp+kkvcgVt5wjXjkMMS
yuq6ihn73wY0QGBnDXX+IVO0UDBhJxzxzwcXkPz7y/f65LiaBA47EBLPjhKjulB6
+E0Ok3refH2i44U5BSIn/2u5R2xzgED7IO/1EBBwfquc68c870ADm7CT8bTVu56U
-----END RSA PRIVATE KEY-----
"""
# private_key_file = StringIO()
# private_key_file.write(str_key)
# private_key_file.seek(0)
# private_key = paramiko.RSAKey.from_private_key(private_key_file)


# with open("./chave.pem", "w") as text_file:
#     text_file.write(str_key)


# sftp_conector = SftpConector(brl_host, 22, brl_username, brl_password, "./chave.pem")
# sftp_conector.open_conection()

str_key = get_text_by_key('captalys-analytics-land-production', 'brl/Key/sftp/v1/key-pem/acessoBRL.pem')

print(f"Arquivo do Wanderson {type(str_key)}")

private_key_file = StringIO()
private_key_file.write(str_key)
private_key_file.seek(0)
private_key = paramiko.RSAKey.from_private_key(private_key_file, brl_password)
sftp_conector = SftpConector(brl_host, 22, brl_username, brl_password, private_key)
sftp_conector.open_conection()
