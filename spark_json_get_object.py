import pyspark
from pyspark.sql import SparkSession 
from pyspark.sql.functions import *
from pyspark.sql import Window
from pyspark.sql.types import IntegerType, DoubleType, StringType, DataType, TimestampType, LongType, MapType, ArrayType, StructType, StructField
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




def parse_json(array_str):
    json_obj = json.loads(array_str)
    yield(json_obj["Zipcode"], json_obj["ZipCodeType"])
    #for item in json_obj:
    #    yield (item["Zipcode"], item["ZipCodeType"])

spark=SparkSession.builder.appName("stringoperations").getOrCreate()
data = [
  (1,2,'''{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}'''),
  (1,2,'''{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}'''),
  (1,2,'''{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}''')
  ]
columns=["id","id2", "jsonval"]
df=spark.createDataFrame(data,columns)

df.printSchema()


df2 = df.select('id','id2','jsonval',get_json_object(col('jsonval'), '$.Zipcode2').alias('zipcode'))


df2.printSchema()
df2.show(truncate=False)