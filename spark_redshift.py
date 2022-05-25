from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession 
from pyspark.sql.functions import *


aws_access_key_id='ASIAS34UIATUMNGWVXMZ'
aws_secret_access_key='Y66sIeXVgTZy/20Zz2cy1xi26TtGz4QjVto4FgeJ'
aws_session_token='IQoJb3JpZ2luX2VjEDoaCXNhLWVhc3QtMSJIMEYCIQCnT/7KMhUCUFXFA1+0VGzaLyOWdUqK98Gg+T1u+3t2owIhAL1hq7pX/DkuGCKQsXchwWHDza2j+MVRKQJV2FDvdXkcKqgDCCMQARoMMTk3MzQyNTI4NzQ0IgznQ844KybMGM7anQ8qhQMbluZfP+2Od3OrG/ii50ZAQ+FcVtWboOYifKOGwQKIfi7VIldxourrerRfw0u5VmwKkDh9iXyVhq4/wX4avlEWuLx534C54RbwXZW6FW9MX42HnjEporpFG2WDDtMlRsQ0pYmcNQJpsfG8R6bmXSyPv6tqUWcu33p2bjAKhZP7v8kJDeoZdmZ02UVC2NWGjyEUNlrDRodd2fld3E+BjETKiqpJcfiPxwHw603vQ5Phx85X40ybcvKIqI3tykY8qiWXQRgZbhvC/ZdF5UxouFfcv707shnUOoaUqjkrlLCTMsvncIb/kqMCi3f24RqYuGZRcbESx5d0OzeTsppsFYkW45yMn0rL3yF9QxJDOMRtJA7Od5EdlP8lVfCc+jnaN2qIYRzS5oTYN/LWpzbDL7BgxEe1Nvo2jvue11bHGu6u+wiJLQ8mQBlC56feM4Vn4conEGjx5WzBALJLVjbhiMFOoSLWvEFNlcPIyp4aeVERdGfXprBTWD/GrcyZ+5FLh9IbNDZ2QjDakKGUBjqlAUlhpS0Ucnivc96csU0XI4vfE7+pTr3jZOFtJ9d1WeAuf1EuBtIMBIf3Fdg0NG6BmVdZ/wEiM/gqkk03MMxKDPq8N9Gj/GV83c2B7KmV/OTd2mTryC3oBMM+r7USa0CUAdVdwKrE49ckV7TcxP9tF9p93OngULUmgGDj8Zxrf42wHGcfJsBRWziYy4uhMCyWE9vWMZDEWwgq1LYHpXzQlCoyIQtgXg=='

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

'''
    "/home/herculano/dev/java/jars/aws-java-sdk-core-1.12.23.jar",
    "/home/herculano/dev/java/jars/aws-java-sdk-redshift-1.12.23.jar",
    "/home/herculano/dev/java/jars/aws-java-sdk-redshift-arcadia-internal-1.0.jar",
    "/home/herculano/dev/java/jars/aws-java-sdk-redshift-internal-1.12.x.jar",
    "/home/herculano/dev/java/jars/aws-java-sdk-sts-1.12.23.jar",
    "/home/herculano/dev/java/jars/commons-codec-1.15.jar",
    "/home/herculano/dev/java/jars/commons-logging-1.2.jar",
    "/home/herculano/dev/java/jars/httpclient-4.5.13.jar",
    "/home/herculano/dev/java/jars/httpcore-4.4.13.jar",
    "/home/herculano/dev/java/jars/jackson-annotations-2.12.3.jar",
    "/home/herculano/dev/java/jars/jackson-core-2.12.3.jar",
    "/home/herculano/dev/java/jars/jackson-databind-2.12.3.jar",
    "/home/herculano/dev/java/jars/jackson-dataformat-cbor-2.12.3.jar",
    "/home/herculano/dev/java/jars/joda-time-2.8.1.jar",
    "/home/herculano/dev/java/jars/redshift-jdbc42-2.1.0.7.jar"
'''
jars = [
    "/home/herculano/dev/java/jars/aws-java-sdk-core-1.12.23.jar",
    "/home/herculano/dev/java/jars/aws-java-sdk-redshift-1.12.23.jar",
    "/home/herculano/dev/java/jars/aws-java-sdk-redshift-arcadia-internal-1.0.jar",
    "/home/herculano/dev/java/jars/aws-java-sdk-redshift-internal-1.12.x.jar",
    "/home/herculano/dev/java/jars/aws-java-sdk-sts-1.12.23.jar",
    "/home/herculano/dev/java/jars/commons-codec-1.15.jar",
    "/home/herculano/dev/java/jars/commons-logging-1.2.jar",
    "/home/herculano/dev/java/jars/httpclient-4.5.13.jar",
    "/home/herculano/dev/java/jars/httpcore-4.4.13.jar",
    "/home/herculano/dev/java/jars/jackson-annotations-2.12.3.jar",
    "/home/herculano/dev/java/jars/jackson-core-2.12.3.jar",
    "/home/herculano/dev/java/jars/jackson-databind-2.12.3.jar",
    "/home/herculano/dev/java/jars/jackson-dataformat-cbor-2.12.3.jar",
    "/home/herculano/dev/java/jars/joda-time-2.8.1.jar",
    "/home/herculano/dev/java/jars/redshift-jdbc42-2.1.0.7.jar"
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