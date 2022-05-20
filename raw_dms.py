#!/usr/bin/env python3
#-*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, DateType, TimestampType
#from pyspark.sql.functions import current_timestamp, input_file_name
from pyspark.sql.functions import *

from datetime import date
from awsglue.utils import getResolvedOptions
from functools import partial
import datetime
from datetime import datetime, timedelta
import boto3
import json
import sys
import logging
from pyspark.sql.types import IntegerType, DoubleType, StringType, DataType, TimestampType, LongType
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['source_bucket', 'destination_bucket', 'domain', 'load_type', 'sgbd','database', 'schema', 'table', 'date'])



def cast_dataframe_schema(df):
    # bucket = args["source_bucket"]
    # client = boto3.client('s3')
    # file = f'{get_file_key(args, "schema")}.json'
    
    # result = client.get_object(Bucket=bucket, Key='cci/schema/dms/v1/json/lake/postgres/geral/sci/public/parcela.json') 
    # text = result["Body"].read().decode()
    # schema = json.loads(text)
    
    for coldtypes in df.dtypes:
        fieldname = coldtypes[0]
        fieldtype = coldtypes[1]
        
        print(f"fieldname: {fieldname}, fieldtype: {fieldtype}")
        

        if 'sys_commit_time' in fieldname: # sys_commit_time or sys_commit_timestamp
            df = df.withColumn(fieldname, col(fieldname).cast(TimestampType()))
        elif 'timestamp' in fieldtype or 'date' in fieldtype:
            df = df.withColumn(fieldname, col(fieldname).cast(StringType()))
        

    return df    
    
def load_raw():
    
    sparkContext = SparkContext()
    glueContext = GlueContext(sparkContext)
    spark = glueContext.spark_session
    
    print('Init load spark')

    
    #read data landzone
    source_path = get_source_path(args, 'data')
    df_land = (
        spark
        .read
        .format("parquet")
        .option("header", True)
        .option("inferSchema", True)
        .load(f's3a://{args["source_bucket"]}/{source_path}/*.parquet')
    )
    '''
    source_path = get_source_path(args, 'data')
    dyf_land = glueContext.create_dynamic_frame_from_options(
            connection_type="s3",
            connection_options = {
                "paths": [f's3://{args["source_bucket"]}/{source_path}/*.parquet']
            },
            format="parquet",
            transformation_ctx="dyf_land")
    dyf_land.printSchema()
    df_land = dyf_land.toDF()
    '''      
            

    
    sys_commit_time = current_timestamp()
    sys_file_name = input_file_name()
    sys_operation = 'R'
    
    if args["load_type"] == "full":
        print('if load full')
        df_land.printSchema()
        df_land_insert = df_land.select(lit(sys_operation).alias("sys_operation"),
                                        lit(sys_commit_time).alias("sys_commit_time"), 
                                        lit(sys_file_name).alias("sys_file_name"),"*")
    else:
        print('else load')
        df_land.printSchema()
        df_land_insert = df_land.withColumnRenamed("Op","sys_operation").select(lit(sys_commit_time).alias("sys_commit_time"), 
                                                                                lit(sys_file_name).alias("sys_file_name"),"*")
    print('Cast dataframe')
    df_land_converted = cast_dataframe_schema(df_land_insert)  
    
    dyf_land_converted = DynamicFrame.fromDF(df_land_converted , glueContext, 'dyf_land_converted')
    
    # write data rawzone
    print('Write dataframe')
    target_path = get_target_path(args, 'data')
    glueContext.write_dynamic_frame.from_options(
                    frame = dyf_land_converted,
                    connection_type='s3',
                    connection_options = {"path": f's3://{args["destination_bucket"]}/{target_path}'},
                    format="parquet"
                )
                
    '''
    target_path = get_target_path(args, 'data')
    (
       df_land_converted
            .repartition(10)
            .write
            .mode("overwrite")
            .format("parquet")
            .save(f's3a://{args["destination_bucket"]}/{target_path}')
      )  
      
    '''

# get path source data
def get_source_path(args, typ): 
    if args["load_type"] == "full":
        path_bucket = f'{args["domain"]}/{typ}/dms/v1/parquet/lake/{args["sgbd"]}/geral/{args["database"]}/{args["schema"]}/{args["table"]}/'
    else:
        date_path = args["date"].replace("-", "/")
        path_bucket = f'{args["domain"]}/{typ}/dms/v1/parquet/lake/{args["sgbd"]}/geral/{args["database"]}/{args["schema"]}/{args["table"]}/{date_path}'    
    return path_bucket
    
# get path target data   
def get_target_path(args, typ): 
    if args["load_type"] == "full":
        yesterday = datetime.now() - timedelta(1) 
        date_full = yesterday.strftime('%Y-%m-%d')
        path_bucket_target = f'{args["domain"]}/{typ}/dms/v1/parquet/lake/{args["sgbd"]}/geral/{args["database"]}/{args["schema"]}/{args["domain"]}_{args["database"]}_{args["schema"]}_{args["table"]}/sys_file_date={date_full}'
    else:
        path_bucket_target = f'{args["domain"]}/{typ}/dms/v1/parquet/lake/{args["sgbd"]}/geral/{args["database"]}/{args["schema"]}/{args["domain"]}_{args["database"]}_{args["schema"]}_{args["table"]}/sys_file_date={args["date"]}'  
    return path_bucket_target
    
if __name__ == '__main__': 
    load_raw()