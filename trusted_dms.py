import sys
import boto3
import json
import logging

from awsglue.utils import getResolvedOptions
import pyspark.sql.functions as f
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import IntegerType, DoubleType, StringType, DataType, TimestampType, LongType
import re
import pg8000 as pg


def get_connection(conn_name):
    client = boto3.client('glue')
    response = client.get_connection(Name=conn_name)
    
    # https://docs.aws.amazon.com/glue/latest/dg/connection-defining.html#connection-properties-jdbc
    jdbc_url = response['Connection']['ConnectionProperties']['JDBC_CONNECTION_URL'].split('/')
    database_port = jdbc_url[2].split(':')
    
    conn_param = {}
    conn_param['url'] = response['Connection']['ConnectionProperties']['JDBC_CONNECTION_URL']
    conn_param['database'] = jdbc_url[3]
    conn_param['hostname'] = database_port[0]
    conn_param['port'] = database_port[1]
    conn_param['user'] = response['Connection']['ConnectionProperties']['USERNAME']
    conn_param['password'] = response['Connection']['ConnectionProperties']['PASSWORD']
    conn_param['redshiftTmpDir'] = 's3://captalys-analyticsrecebiveis-raw-production/recebiveis/platform-cessao/tempdir'
    
    return conn_param
    
def get_table_schema(args, client):
    schema_info = {}
    schema_info['col'] = []
    schema_info['pk'] = []
    
    bucket = args["land_bucket"]
    file = f'{get_file_key(args, "schema")}.json'
    print(f"file : {file}")
    result = client.get_object(Bucket=bucket, Key=file) 
    text = result["Body"].read().decode()
    schema = json.loads(text)
    
    for f in schema:
        schema_info['col'].append(f['column_name'])
        

        if f['primary_key']:
            schema_info['pk'].append(f['column_name'])
                
    schema_info['col'].append('sys_commit_time')
    schema_info['col'].append('sys_file_date')
    schema_info['col'].append('sys_file_name')
    schema_info['col'].append('sys_operation')
    schema_info['col'].append('sys_commit_timestamp')

                
    return schema_info

def get_file_key(glue_args, conf):
    path_key = f"{glue_args['domain']}/{conf}/{glue_args['technology']}/v1/json/lake/{glue_args['sgbd']}/{glue_args['instance']}/{glue_args['database']}/{glue_args['schema']}/{glue_args['table']}"
    # print(f"path_key: {path_key}")
    return path_key
    
def get_column_types_df(df):
    lst_column_types = []
    for col in df.dtypes:
        lst_column_types.append({'col': col[0], 'type': col[1]})
    return lst_column_types
    
def cast_dataframe_schema(args, client, dfin):
    bucket = args["land_bucket"]
    file = f'{get_file_key(args, "schema")}.json'
    
    result = client.get_object(Bucket=bucket, Key=file) 
    text = result["Body"].read().decode()
    schema = json.loads(text)
    
    df=dfin.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in dfin.columns])
        
    for column in schema:
        fieldname = column['column_name']
        fieldtype = column['data_type']
        
        # df.printSchema()
        
        # print(f"fieldname: {fieldname}, fieldtype: {fieldtype}")
        

        if 'int' in fieldtype or 'integer' in fieldtype:
            df = df.withColumn(fieldname, col(fieldname).cast(IntegerType()))
            
        elif 'serial' in fieldtype or 'bigint' in fieldtype:
            df = df.withColumn(fieldname, col(fieldname).cast(LongType()))
            
        elif 'numeric' in fieldtype or 'decimal' in fieldtype or \
            'double' in fieldtype or 'float' in fieldtype or \
            'real' in fieldtype or 'money' in fieldtype or \
            'currency' in fieldtype:
            df = df.withColumn(fieldname, col(fieldname).cast(DoubleType()))
        elif 'sys_commit_time' in fieldname:
            df = df.withColumn(fieldname, col(fieldname).cast(TimestampType()))
        elif 'text' in fieldtype:
            # df = df.withColumn(fieldname, udf_text_value(col(fieldname)))
            df = df.withColumn(fieldname, col(fieldname).cast(StringType()))
            df = df.withColumn(fieldname, regexp_replace(col(fieldname),r"\r\n\t", " "))
            df = df.withColumn(fieldname, regexp_replace(col(fieldname),"|", ""))
            df = df.withColumn(fieldname, trim(col(fieldname)))
            df = df.withColumn(fieldname, substring(col(fieldname),1,15000))
        else:
            df = df.withColumn(fieldname, col(fieldname).cast(StringType()))
            df = df.withColumn(fieldname, regexp_replace(col(fieldname),"|", ""))
            df = df.withColumn(fieldname, trim(col(fieldname)))

    return df
    
#V2 Herculano    
def get_bucket(args, typ):
    if args["source_bucket"] == "captalys-analytics-land-production":
        if (args["database"] == "salesforce"):
            path_bucket = f'salesforce/{typ}/api/v54.0/json/lake/api/{args["database"]}/{args["schema"]}/master/{args["table"]}'
        else:   
            path_bucket = f'captalys/{typ}/collector/v1/json/lake/postgres/cessao/{args["database"]}/{args["schema"]}/{args["table"]}'
    else:
        path_bucket = f'{args["domain"]}/{typ}/{args["database"]}/{args["schema"]}/{args["table"]}'    
    return path_bucket
    

def get_result_query(connection_name, str_qry):
    conn_param = get_connection(connection_name)    
    conn = pg.connect(
        database=conn_param['database'],
        host=conn_param['hostname'],
        port=conn_param['port'],
        user=conn_param['user'],
        password=conn_param['password']
    )
    conn.autocommit = False
    cur = conn.cursor()
                
    cur.execute(str_qry)
    rows = cur.fetchall()
                
    cur.close()
    cur.close()
    
    return rows
    
def exec_query_with_commit(connection_name, str_qry):
    conn_param = get_connection(connection_name)    
    conn = pg.connect(
        database=conn_param['database'],
        host=conn_param['hostname'],
        port=conn_param['port'],
        user=conn_param['user'],
        password=conn_param['password']
    )
    cur = conn.cursor()
    
    cur.execute(str_qry)
    conn.commit()

                
    cur.close()
    cur.close()
    
def delete_files(bucket_name, prefix):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix=prefix):
        s3.Object(bucket.name,obj.key).delete()

def add_sys_file_date(value):
   return str(value)


@udf(returnType=StringType()) 
def get_spark_column_str(str):
    # return str.upper()
    return str 
    
@udf(returnType=StringType())     
def udf_text_value(value):
    value_without_newline = re.sub(r"\r\n\t", " ", value)
    value_without_quote = value_without_newline.replace('\"', '')
    return value_without_quote[0:16380]

    
if __name__ == '__main__':
    client = boto3.client('s3')
    
    print('Iniciado')
    args = getResolvedOptions(sys.argv, [
        'date',
        'source_bucket',
        'land_bucket',
        
        'domain',
        'technology',
        'sgbd',
        'instance',
        'database',
        'schema',
        'table'
    ])
    
    if args['domain'] == 'cci':
        bucket_temporary = 'captalys-analytics-temporary'
        table = f"cci_{args['database']}_{args['schema']}_{args['table']}"
        prefix = f"{args['domain']}/data/{args['technology']}/v1/parquet/lake/{args['sgbd']}/{args['instance']}/{args['database']}/{args['schema']}/{table}/sys_file_date={args['date']}/"
        
        s3_path = f"s3://{args['source_bucket']}/{prefix}"
        s3_path_temp = f"s3://{bucket_temporary}/{prefix}"
    else:
        table = f'{args["database"]}_{args["schema"]}_{args["table"]}'
    table = table.replace('-', '_')
    
    print(f'Processing table {table}')
    print(f'Reading path {s3_path}')
    

    str_cmd_schema = f"select column_name, data_type from svv_columns c where table_schema = 'captalys_trusted' and table_name = '{table}' order by ordinal_position "
    rows_schema = get_result_query('asgard-redshift-production-informationSchema',str_cmd_schema)
                
    cols_list_information_schema = ''
    list_information_schema = []
    if rows_schema:
        for row in rows_schema:
            if cols_list_information_schema:
                cols_list_information_schema += ',' + row[0]
            else:
                cols_list_information_schema = row[0] 
            list_information_schema.append(row[0])
    
    sparkContext = SparkContext()
    glueContext = GlueContext(sparkContext)
    spark = glueContext.spark_session
    # spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
    
    print("leitura dos parquets raw")
    dynamic_frame_from_s3 = glueContext.create_dynamic_frame_from_options(
            connection_type="s3",
            connection_options = {
                "paths": [s3_path]
            },
            format="parquet",
            transformation_ctx="dynamic_frame_from_s3")
            
    if dynamic_frame_from_s3 and dynamic_frame_from_s3.count() > 0:
        print('Existe dataframe')
        table_schema = get_table_schema(args, client)
        
        pk = ''
        pk_list_json_schema = table_schema['pk']
        if pk_list_json_schema:
            pk = ','.join(pk_list_json_schema)
        else:
            logging.info('Tabela sem chave primaria. Skipped.')
                
        if pk:
            print('Existe PK')
            dynamic_frame_from_s3.printSchema()
            data_frame_raw_s3 = dynamic_frame_from_s3.toDF()
            str_sys_file_date = args['date']
            data_frame_raw_s3_2 = data_frame_raw_s3.withColumn('sys_file_date', get_spark_column_str(lit(str_sys_file_date)))
            

            print('Conversao da raw')
            data_frame_raw = cast_dataframe_schema(args, client, data_frame_raw_s3_2)
            
            partition_order = Window.partitionBy(pk).orderBy(desc('sys_commit_timestamp'))
            print('raw row number and drop dataframe')
            dataFrame_raw_lastest = data_frame_raw.withColumn('rn', row_number().over(partition_order)).filter('rn = 1').drop('rn')
            dataFrame_raw_lastest_v2 = dataFrame_raw_lastest.filter("sys_operation != 'D'")
            dataFrame_raw_lastest_v3 = dataFrame_raw_lastest_v2.withColumn('sys_commit_timestamp',date_format(col("sys_commit_timestamp"), "yyyy-MM-dd HH:mm:ss.SSS"))
            dataFrame_raw_lastest_v4 = dataFrame_raw_lastest_v3.withColumn('sys_commit_timestamp',col('sys_commit_timestamp').cast(TimestampType()))
            
            dataFrame_raw_lastest_ordened = dataFrame_raw_lastest_v4.select(*list_information_schema)
            
            
            
            print('raw dynamic frame')
            dynamicFrame_raw_lastest = DynamicFrame.fromDF(dataFrame_raw_lastest_ordened, glueContext, 'dynamicFrame')
            print(f"tamanho da raw")
           
           
            
            
            # trusted
            conn_param_trusted = get_connection('asgard-redshift-production-trusted-owner')
            conn_param_trusted['query'] = f"SELECT * FROM captalys_trusted.{table}"
            print('Buscando dataframe trusted dynamic')
            dynamicFrame_trusted = glueContext.create_dynamic_frame.from_options('redshift', conn_param_trusted)
            
            print('Dataframe trusted buscado')
            
            if dynamicFrame_trusted and dynamicFrame_trusted.count() > 0:
                
                print('Contem dados na trusted')
                
                
                dynamicFrame_trusted.printSchema()
                
                dataFrame_trusted = dynamicFrame_trusted.toDF()
                dataFrame_trusted = cast_dataframe_schema(args, client, dataFrame_trusted)
                
                # adiciona colunas na raw caso não exista
                for column in [column for column in dataFrame_trusted.columns if column not in dataFrame_raw_lastest.columns]:
                    dataFrame_raw_lastest = dataFrame_raw_lastest.withColumn(column, lit(None))
                
                # remove others columns   
                for column in [column for column in dataFrame_raw_lastest.columns if column not in dataFrame_trusted.columns]:
                    dataFrame_raw_lastest.drop(col(column))
                    
                print('merge frames') 
                dataframe_merged=dataFrame_raw_lastest.unionByName(dataFrame_trusted)
                print(f"MERGED FRAMES ")
                print('oder by merged frames')
                partition_order = Window.partitionBy(pk).orderBy(desc('sys_commit_timestamp'))
                print('row number merged frames')
                dataframe_merged_lastest = dataframe_merged.withColumn('rn', row_number().over(partition_order)).filter('rn = 1').drop('rn')
                dataframe_merged_lastest_v2 = dataframe_merged_lastest.filter("sys_operation != 'D'")
                dataframe_merged_lastest_v3 = dataframe_merged_lastest_v2.withColumn('sys_commit_timestamp',date_format(col("sys_commit_timestamp"), "yyyy-MM-dd HH:mm:ss.SSS"))
                dataframe_merged_lastest_v4 = dataframe_merged_lastest_v3.withColumn('sys_commit_timestamp',col('sys_commit_timestamp').cast(TimestampType()))
            
                print(f"dataframe_merged_lastest OVER PARTITION")
                print('to dynamic frame merged')
                dataframe_merged_lastest_ordened = dataframe_merged_lastest_v4.select(*list_information_schema)
                dynamicFrame_merged = DynamicFrame.fromDF(dataframe_merged_lastest_ordened, glueContext, 'dynamicFrame_merged')
                
                
            else:
                # Somente se não existir na trusted
                print('Merged frame else')
                dynamicFrame_merged = dynamicFrame_raw_lastest
                
                            
            print('Deletando arquivos no bucket')
            delete_files(bucket_temporary, prefix)

            # dynamicFrame_merged.printSchema()

            '''
            glueContext.write_dynamic_frame.from_options(
                frame = dynamicFrame_merged,
                connection_type='s3',
                connection_options = {"path": f'{s3_path_temp}'},
                format="parquet"
            )
            '''
            print('Escrevendo os csvs')
            glueContext.write_dynamic_frame.from_options(
                frame = dynamicFrame_merged,
                connection_type='s3',
                connection_options = {"path": f'{s3_path_temp}'},
                format="csv",
                format_options={
                    "separator": "|"
                }
            )

            print('writed merged frames in S3')
            
            
            print('Truncate stage')
            str_cmd_truncate = f"truncate table captalys_stage.{table}"
            exec_query_with_commit('asgard-redshift-production-stage-owner', str_cmd_truncate)



            print('init command copy')

            str_cmd_copy = f"""
            COPY captalys_stage.{table} (
             {cols_list_information_schema}
            )
            FROM
              '{s3_path_temp}' 
            IAM_ROLE 'arn:aws:iam::197342528744:role/CaptalysRedshiftCustomizable' 
            DELIMITER as '|' FORMAT AS csv IGNOREHEADER 1;
            """
            exec_query_with_commit('asgard-redshift-production-stage-owner', str_cmd_copy)
            print('end command copy')


            str_cmd = f"""
            begin;
                truncate table captalys_trusted.{table};

                insert into captalys_trusted.{table} ({cols_list_information_schema}) 
                select {cols_list_information_schema} from captalys_stage.{table};

            commit;"""

            print('select into command') 
            exec_query_with_commit('asgard-redshift-production-trusted-owner', str_cmd)
            print('SELECT INTO EXECUTED')

            print('Truncate stage')
            str_cmd_truncate = f"truncate table captalys_stage.{table}"
            exec_query_with_commit('asgard-redshift-production-stage-owner', str_cmd_truncate)
            print('end')
                
    else:
        print('Não existe dataframe')
    
    

    
    
    
    
    
    
    