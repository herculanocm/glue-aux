from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession 
from pyspark.sql.functions import *


# aws_access_key_id='ASIAS34UIATULKN3245I'
# aws_secret_access_key='UXATF7+NaCOKeiCsM9lauLJxJh59hHMPa+JkzYZm'
# aws_session_token='IQoJb3JpZ2luX2VjEDMaCXNhLWVhc3QtMSJHMEUCIDLfP7ESaW+4aovVZavGUm9z7jGC+tj2TG4ZMpQ0qOwyAiEAm1vGMmdYhvMLQR/v1eX5enJ4C4ZrG52grq/ThD/fl3YqqAMIHBABGgwxOTczNDI1Mjg3NDQiDNYe+vt4UCJL7N4EiSqFA2OCtYEkasENYZPuM86juBm5LTyREHpPrOrgoYz4XqKxsBTmqAQXDqig5BpWNiavXvC+I60jCrpYE8okukDSQgW0OM5lox0LznJ3pgdr2lWBk6C5MmbFfZCt9Qyys9WZ95DMvVqXL2UW8RswFAMLlgnmDxJel8kn3x9EBtTiqeuaIN6TV9HF/Q1cORWbhktZ6yngPZJYNO1vR5jbfcOVEBNmqavJ29BPJw9RmwjWh4nP7QhV5qcLZSzZ4E5F/sjc2RXxxmJQ2n/ZsJPp5AtSAjQCQ/YQL/vTxxoVB4W+THAtOvQQJKFVmFMwbT2ay+tRT+/4lC9XDpOIyWe2tRy6WvQZ5fUD7uFrcNFPrYMcNCrATLD+18iRMSuVkDzLplb5aF4VJ3jNpRPe48SiExzYUaQ6+dP4p0zfDeQG1opL6akskdVPt6q/wJh0dKXH2ta0v5BBRKTyWf1ZsC6oocGGpgInvCHxrSNXgYjFJQWqfD7Ci7kqZiQLWrmKrtRvwh+0OCc00uihMO7Hn5QGOqYB+3DiMpFW2nMLvF6UIbTaV68tHWF7AFQEpQPkjORNgZ6lXEj1qKc/nOmFEUn81zeGqHJ56LP1pHqfhQQCIoxAgirPy2cMV2rdAzH4iH1zRJGwNCjCexoVR+Z/Jiie3y3dMsWbqSskPk33QCZEWthORRfNX3Z+KchIJiLvm4ZV563DKD/HhACU+/fdFGeDRB6q97xu1hisLQ2G2uvEKuxFB1L8tpXZAw=='

aws_access_key_id='ASIAS34UIATUC2D7J7MY'
aws_secret_access_key='QlS4JjZIkycz4PRVZ08He9K/qy4fCcDroO9VipQ7'
aws_session_token='IQoJb3JpZ2luX2VjEDQaCXNhLWVhc3QtMSJGMEQCICeaECqe8tuwcw9kZEkVs6eiFC7/XXJdjEdhUP6su2qgAiA+mGDWCgmTuFALpz9fTYgCxJDfsmsE59MZqt/1RdfhoiqiAwgcEAEaDDE5NzM0MjUyODc0NCIMCkzYmfK4qsfeDrilKv8Ck3TNlVmU2Knosib49u9XwvdLPvvn0PzaRndwQ2cmmBBFpDgP4cvdUDWCtolfg4FBh6iILydPjYlBc1LCPwBgxn17982qLdBRevxLge/gaHLrl1dvN8GI4Eclcm9YHgmsNCo7g3G+1YjA8sIUFH5W07Gsp8q/P8XoIknI+gPMxpC2TcNLL4l3xpgZQGZQk4l/az3X1dFNSe23TC6dIsosOiYWtvcLVNzsf6QTwDCTuFImHUUnWH1YE9HYbwQe7TWIrGlZ3GDMhDgsHPASZHcgLJHFvWEdYzkaPiI7XZ9TgEGCrYl+T0mDF5z/XtxluCXxcGi+qbDSG8bNexYCmD5imiagEM9Z74NiHzkwKu5WksGo8kVHJiYMKqVy+TRU8KNIgAtjGSAsp5pSAeIegmbNSy3Hf7hjrRLj9vNoDBDnUNuSijgNzsxfjiFHtFuNQ4coM62rrU4OmDV0TxHPGh2ZsdYsSj3GY7aRfzIdSlNaXy4mc0/FmOENkwsHtunxCyAwldSflAY6pwHbKezyPAEe9pCyuLcRtNNbxpUiBWilpEMViZZWNVPnzx1JBM0w71auP+0B/n14uA7bq0XD7lloiY0uuoTfp1QfLCIjQ/N0ezS4KjAaLOuKrw8Q5+DDRSXmgmbrH5g63olIyJsp/+jcAQ+8vcYpLtXPTerl4Yz3xtFt6DQ68XSqA0qlVp3m2al9OO81rTuP7XDOcnFtp6375QDsXn5YJbyRQju0ySb/lw=='

config = {
    'aws_access_key': aws_access_key_id,
    'aws_secret_key': aws_secret_access_key,
    'aws_session_token': aws_session_token,
    'aws_region': 'sa-east-1',
    'aws_bucket': 'captalys-analytics-temporary',
    'redshift_user': 'ivan_marques',
    'redshift_pass': 'vl6i36K7mxoQw8MeqNMq',
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