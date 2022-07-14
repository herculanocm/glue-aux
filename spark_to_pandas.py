import pyspark
from pyspark.sql import SparkSession 
from pyspark.sql.functions import *
from pyspark.sql import Window
from pyspark.sql.types import IntegerType, DoubleType, StringType, DataType, TimestampType, LongType, MapType, ArrayType, StructType, StructField
import json
import pandas
import ast


spark=SparkSession.builder.appName("stringoperations").getOrCreate()
data = [
  (1,2,'''{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR", "nobj": {"cod": 1}}'''),
  (1,2,'''{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR", "nobj": {"cod": None}}'''),
  (1,2,'''{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR", "nobj": {"cod": 2}}''')
  ]
columns=["id","id2", "jsonval"]
df=spark.createDataFrame(data,columns)

df_pandas = df.toPandas()

df_pandas["jsonval"] =  df_pandas["jsonval"].map(lambda d : ast.literal_eval(d))
df_pandas["jsonval2"] = df_pandas["jsonval"].map(lambda d : d['nobj']['cod'])
print(df_pandas.dtypes)
print(df_pandas.head())

