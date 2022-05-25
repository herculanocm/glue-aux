import pyspark
from pyspark.sql import SparkSession 
from pyspark.sql.functions import *
from pyspark.sql import Window
from pyspark.sql.types import IntegerType, DoubleType, StringType, DataType, TimestampType, LongType

spark=SparkSession.builder.appName("stringoperations").getOrCreate()
data = [
  (1,2,'2021-01-02 12:12:12'),
  (1,2,'2021-01-02 11:12:12'),
  (1,2,'2021-01-02 10:12:12'),
  (1,3,'2021-01-02 12:12:12'),
  (1,3,'2021-01-01 12:12:12'),
  (1,3,'2021-01-01 11:12:12')
  ]
columns=["id","id2", "sys_commit_timestamp"]
df=spark.createDataFrame(data,columns)

df = df.withColumn("sys_commit_timestamp", col("sys_commit_timestamp").cast(TimestampType()))

partition_order = Window.partitionBy("id","id2").orderBy(desc('sys_commit_timestamp'))
df_filter = df.withColumn('rn', row_number().over(partition_order)) #.filter('rn = 1')

df_filter.printSchema()
df_filter.show(truncate=False)