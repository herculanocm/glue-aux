import pyspark
from pyspark.sql import SparkSession 
from pyspark.sql.functions import *
from pyspark.sql import Window
from pyspark.sql.types import IntegerType, DoubleType, StringType, DataType, TimestampType, LongType

spark=SparkSession.builder.appName("stringoperations").getOrCreate()
data = [
  (1,2,'12.686.424/0001-30'),
  (1,2,'40.912.255/0001-45'),
  (1,2,'08.621.021/0001-36')
  ]
columns=["id","id2", "cnpj"]
df=spark.createDataFrame(data,columns)

df2 = df.withColumn("cnpj", regexp_replace(col('cnpj'), '[^0-9]', ''))


df2.printSchema()
df2.show(truncate=False)