from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession 
from pyspark.sql.functions import *


aws_access_key_id='ASIAS34UIATULKN3245I'
aws_secret_access_key='UXATF7+NaCOKeiCsM9lauLJxJh59hHMPa+JkzYZm'
aws_session_token='IQoJb3JpZ2luX2VjEDMaCXNhLWVhc3QtMSJHMEUCIDLfP7ESaW+4aovVZavGUm9z7jGC+tj2TG4ZMpQ0qOwyAiEAm1vGMmdYhvMLQR/v1eX5enJ4C4ZrG52grq/ThD/fl3YqqAMIHBABGgwxOTczNDI1Mjg3NDQiDNYe+vt4UCJL7N4EiSqFA2OCtYEkasENYZPuM86juBm5LTyREHpPrOrgoYz4XqKxsBTmqAQXDqig5BpWNiavXvC+I60jCrpYE8okukDSQgW0OM5lox0LznJ3pgdr2lWBk6C5MmbFfZCt9Qyys9WZ95DMvVqXL2UW8RswFAMLlgnmDxJel8kn3x9EBtTiqeuaIN6TV9HF/Q1cORWbhktZ6yngPZJYNO1vR5jbfcOVEBNmqavJ29BPJw9RmwjWh4nP7QhV5qcLZSzZ4E5F/sjc2RXxxmJQ2n/ZsJPp5AtSAjQCQ/YQL/vTxxoVB4W+THAtOvQQJKFVmFMwbT2ay+tRT+/4lC9XDpOIyWe2tRy6WvQZ5fUD7uFrcNFPrYMcNCrATLD+18iRMSuVkDzLplb5aF4VJ3jNpRPe48SiExzYUaQ6+dP4p0zfDeQG1opL6akskdVPt6q/wJh0dKXH2ta0v5BBRKTyWf1ZsC6oocGGpgInvCHxrSNXgYjFJQWqfD7Ci7kqZiQLWrmKrtRvwh+0OCc00uihMO7Hn5QGOqYB+3DiMpFW2nMLvF6UIbTaV68tHWF7AFQEpQPkjORNgZ6lXEj1qKc/nOmFEUn81zeGqHJ56LP1pHqfhQQCIoxAgirPy2cMV2rdAzH4iH1zRJGwNCjCexoVR+Z/Jiie3y3dMsWbqSskPk33QCZEWthORRfNX3Z+KchIJiLvm4ZV563DKD/HhACU+/fdFGeDRB6q97xu1hisLQ2G2uvEKuxFB1L8tpXZAw=='


config = {
    'aws_access_key': aws_access_key_id,
    'aws_secret_key': aws_secret_access_key,
    'aws_session_token': aws_session_token,
    'aws_region': 'sa-east-1',
    'aws_bucket': 'captalys-analytics-temporary',
    'redshift_user': 'app_trusted_owner',
    'redshift_pass': 'C7WlpDKQKjqmjpmCnF50',
    'redshift_port': 5439,
    'redshift_db': 'datalake_dw',
    'redshift_host': 'asgard-redshift-production.cmqegk5gj3mi.sa-east-1.redshift.amazonaws.com',
}


jars = [
    "/home/herculano/dev/py/redshift_spark/redshift-jdbc42-2.1.0.7.jar"
]

conf = (
    SparkConf()
    .setAppName("S3 with Redshift")
    .set("spark.driver.extraClassPath", ":".join(jars))
    .set("spark.hadoop.fs.s3a.access.key", config.get('aws_access_key'))
    .set("spark.hadoop.fs.s3a.secret.key", config.get('aws_secret_key'))
    .set("spark.hadoop.fs.s3a.session.token", config.get('aws_session_token'))
    .set("spark.hadoop.fs.s3a.path.style.access", True)
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .set("com.amazonaws.services.s3.enableV4", True)
    .set("spark.hadoop.fs.s3a.endpoint", f"s3-{config.get('region')}.amazonaws.com")
    .set("spark.executor.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true")
    .set("spark.driver.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true")
)
sc = SparkContext(conf=conf).getOrCreate()
sqlContext = SQLContext(sc)


schema = 'captalys_trusted'
table = 'platform_cessao_master_cedente_processo'


df = sqlContext.read \
               .format("jdbc") \
               .option("url", f"jdbc:redshift://{config.get('redshift_host')}:{config.get('redshift_port')}/{config.get('redshift_db')}") \
               .option("dbtable", "(select * from captalys_trusted.platform_cessao_master_cedente_processo limit 100) foo") \
               .option("user", config.get('redshift_user')) \
               .option("password", config.get('redshift_pass')) \
               .load()

df.show()