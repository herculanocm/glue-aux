import pyspark
from pyspark.sql import SparkSession 
from pyspark.sql.functions import *
spark=SparkSession.builder.appName("stringoperations").getOrCreate()
data = [(1,"20200828", "G1 | ESCADA 1O P/ 2O | STAND | FRALDÁRIO"),(2,"20180525", "G1 | ESCADA 1O P/     2O | STAND | FRALDÁRIO     ")]
columns=["id","date", "unidade"]
df=spark.createDataFrame(data,columns)

reg_exp="\\s+"

#Using SQL function substring()
df = df.withColumn("unidade", regexp_replace(col("unidade"),'|', '')) \
            .withColumn("unidade", trim(col("unidade")))
df.printSchema()
df.show(truncate=False)

'''
#Using select    
df1=df.select('date', substring('date', 1,4).alias('year'), \
                  substring('date', 5,2).alias('month'), \
                  substring('date', 7,2).alias('day'))
    
#Using with selectExpr
df2=df.selectExpr('date', 'substring(date, 1,4) as year', \
                  'substring(date, 5,2) as month', \
                  'substring(date, 7,2) as day')

#Using substr from Column type
df3=df.withColumn('year', col('date').substr(1, 4))\
  .withColumn('month',col('date').substr(5, 2))\
  .withColumn('day', col('date').substr(7, 2))
'''
