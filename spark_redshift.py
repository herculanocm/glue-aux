from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession 
from pyspark.sql.functions import *


aws_access_key_id="ASIAS34UIATUJRRR2LJI"
aws_secret_access_key="zZsSYFsSiwGDTwcS+LvwitTxqb73yc68bgtjdJQK"
aws_session_token="IQoJb3JpZ2luX2VjEIv//////////wEaCXNhLWVhc3QtMSJIMEYCIQDUjkyVfHamBz/ffLKfRpv2J0JxhVMnGWCKxu1Ado2fKAIhAPWmrUmgcu42OtXNZHtGgQha1kfGS/o5Z4mjvnVM5bkNKrEDCIT//////////wEQARoMMTk3MzQyNTI4NzQ0Igzv8xTVisfgmfvTahgqhQObKZA9UYP3z0Vuk/qBo8Jz8kM72m20axg3zCkbQ5ARW114y3vcrydlvoLZlPx0H9fIwb6kWEHBgKPLdtQGLd73Q+2ToEhvTkTGvf55Ijghe+cqSR1rR2J0wdgiNbIyRb3M/gD2YMry8sDYKDGzPzEVDLUEsFBiKMKnloijyb1yPu5LtYxDT20SN4l3/VoyydZ/FkERKXSnr1fwhur2ROfuuMZaYihhxCjXDZL5SfdpwLXNFh7d8m5PqKfG7xJDGu/6g7YdcTR6BjMhB43ijG4USks2PUgdOQegOui6PYcKql8GtGunXmGO+5cUW9HYsXkrj2hCi7a7cZB7yeBFFHYZkAM+QjKFzrlQkGTRc6cfyuCnjOBBHG7sW/EX6QYeVjKBFh1BtCUrlVGK/xf+165WI9nKGr9uxC8Bzq99fIPKDYUE+NmChjotmL49O3hc6pN/UINzPZVVXK9HO5aq2+kdpcwn10FnDQFYk1+tEAlskyMEhZFvDgXZmhDpxdFsJt8Q5l2TPzD4huuUBjqlAS1bKyLui1zE+aOWzVVcO9a5L0PRMZh77K2l4T2OPYKCMMyCMvUtqYZmEcgMWm4vp8nR2G/EThyN3RPLFDMmmQ0ms7oh42d/OZdfACEW2DIXQJ0pX1FY9XW+uaWuhsNMTzxtroHkXwkmYQHGEx0QVUYKegtUL4IIcwlmyLLz7oi/7u2crUtb7hiPhFZt09eZZH3PXK9iKBR/EY2pobag5AnPxnDmew=="

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
    "/home/hc/dev/java/jars/aws-java-sdk-core-1.12.23.jar",
    "/home/hc/dev/java/jars/aws-java-sdk-redshift-1.12.23.jar",
    "/home/hc/dev/java/jars/aws-java-sdk-redshift-arcadia-internal-1.0.jar",
    "/home/hc/dev/java/jars/aws-java-sdk-redshift-internal-1.12.x.jar",
    "/home/hc/dev/java/jars/aws-java-sdk-sts-1.12.23.jar",
    "/home/hc/dev/java/jars/commons-codec-1.15.jar",
    "/home/hc/dev/java/jars/commons-logging-1.2.jar",
    "/home/hc/dev/java/jars/httpclient-4.5.13.jar",
    "/home/hc/dev/java/jars/httpcore-4.4.13.jar",
    "/home/hc/dev/java/jars/jackson-annotations-2.12.3.jar",
    "/home/hc/dev/java/jars/jackson-core-2.12.3.jar",
    "/home/hc/dev/java/jars/jackson-databind-2.12.3.jar",
    "/home/hc/dev/java/jars/jackson-dataformat-cbor-2.12.3.jar",
    "/home/hc/dev/java/jars/joda-time-2.8.1.jar",
    "/home/hc/dev/java/jars/redshift-jdbc42-2.1.0.7.jar"
'''
jars = [
    "/home/hc/dev/java/jars/redshift-jdbc42-2.1.0.7.jar"
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