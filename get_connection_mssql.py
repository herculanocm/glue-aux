import boto3


aws_access_key_id='ASIAS34UIATUOVAN3QW6'
aws_secret_access_key='q8sCFUpaVg/YdaKGiyxImEUQjKMZoANE0bgO45vu'
aws_session_token='IQoJb3JpZ2luX2VjEFQaCXNhLWVhc3QtMSJHMEUCIQDHhbmottCo3C881jQKEO71HV78jZgr9e7zjxw4yVlXMQIgQmCJYJIxfa0Pq1uZT58YwswRivb4KQlckWs9YcLDdxMqqAMITRABGgwxOTczNDI1Mjg3NDQiDLTRxb4EbiuCYmjPeSqFA8KYX4KF2xXwVbSUKps7QXm3tthxLvYi3Xdzy7ptCJs7job/YeJ792T02GEwElxgnHdWRjWIKaiG4q91c9C0ZhwjYxKuSgRaQis+vBhB5j7JApj3U/MXtueDJfocQYXgyloPPRWD/XFnHXa5LGbmBl4orOTLFQZvaH3NaBGsoCRFTi4RMHX/nGWJFiG18X6WsSVDEoePv6Kl6eLUFsa+vgngY/3lHaKhUtqtCj3pl7wmMsIM+PPsn2JpEvJWn8W8loZn3NS9bFao0VE1GNZWLX7iouFjs6ENGknEGwz1Gt9Nsem2b3h+vsgKEKNRSxQZ5FoABK2kccEIc4FQiGkIC94P2IHeEiWJw0XZ2TJbLxMMvK0vJsHgu3T+LQdn/geTMyEQ2DJXZcJbn4P/rE8bcAyJubRxF+L8193xuM9arcy6t4I4MXxX8ruG0pD7iP8OD+K9JUWtZHGux8ATRQJl2suFW6x0qrqbJWPBFhLdXTijMyj/HSOvz6XSUTrwm5zTOW5mgcbRMNqH35QGOqYBRNql+xZw1VCYxd/D6z20uNjmZmeze0u0xhavphGcd2VHOR0o1P+IAnNiAjI8XrldCi5fqvFcsujbk4xSvm+Wo812acips0BT530hnfXN/lDxDFBp1kkiSmJovNSZtuYRilYR+MIc0kAOG0bckBFJB0gHElJYzy9V+v2ufNQn5bL3iObnOgu8gjiQ8DZ81TyYUYUb7to/ZYvJN4VS5sMVVtmhtfAZCw=='

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

def get_connection(connection_name):
    response = glue_client.get_connection(Name=connection_name)
    
    # https://docs.aws.amazon.com/glue/latest/dg/connection-defining.html#connection-properties-jdbc
    jdbc_url_str = response['Connection']['ConnectionProperties']['JDBC_CONNECTION_URL']
    jdbc_url = jdbc_url_str.split('/')
    print(f"jdbc: {jdbc_url}")
    host_port_database = jdbc_url[2].split(';')
    print(f"host_port_database: {host_port_database}")
    database = host_port_database[1].split('=')

    host_port = host_port_database[0].split(':')
   
    
    conn_param = {}
    conn_param['database'] = database[1]
    conn_param['hostname'] = host_port[0]
    conn_param['port'] = host_port[1]
    conn_param['user'] = response['Connection']['ConnectionProperties']['USERNAME']
    conn_param['passsword'] = response['Connection']['ConnectionProperties']['PASSWORD']
    
    return conn_param

if __name__ == "__main__":
    print(get_connection('mssql-captalyx-captalys-op'))