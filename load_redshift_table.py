import pandas as pd
import os
import glob
import boto3  
import numpy as np
import pg8000 as pg
import warnings
import math

def read_files(path, separator):
    path = os.getcwd()
    csv_files = glob.glob(os.path.join(path, "*.csv"))

    for f in csv_files:
        
        # read the csv file
        df = pd.read_csv(f, sep=separator, header='infer')
        
    return df

def exec_query_with_commit(connection_name, str_qry):
    conn = pg.connect(
        database='datalake_dw',
        host='asgard-redshift-production.cmqegk5gj3mi.sa-east-1.redshift.amazonaws.com',
        port=5439,
        user='app_trusted_owner',
        password='C7WlpDKQKjqmjpmCnF50'
    )
    cur = conn.cursor()
    
    cur.execute(str_qry)
    conn.commit()

                
    cur.close()
    cur.close()


df = pd.read_csv('/home/hc/dev/py/glue-aux/files/propostas_202206130942.csv', sep='|', header='infer', low_memory=False)

df['sys_commit_time'] = '2022-06-09 00:00:00'
df['sys_file_date'] = '2022-06-09'
df['sys_file_name'] = 's3a//'
df['sys_operation'] = 'r'
df['sys_commit_timestamp'] = '2022-06-09 00:00:00'
df['sys_transaction_id'] = '1'
df['id_controle_data_lake'] = '1'

print('conversão')

df['created']=pd.to_datetime(df['created'].astype(str)).dt.strftime('%Y-%m-%d %H:%M:%S')
df['updated']=pd.to_datetime(df['updated'].astype(str)).dt.strftime('%Y-%m-%d %H:%M:%S')

df['aprovado']=df['aprovado'].astype(str)
df['assinatura_digital']=df['assinatura_digital'].astype(str)
df['score']=df['score'].astype(str)
df['fluxo_medio_meses']=df['fluxo_medio_meses'].astype(str)
df['prazo_minimo']=df['prazo_minimo'].astype(str)
df['spread_prazo']=df['spread_prazo'].astype(str)
df['prazo_esperado']=df['prazo_esperado'].astype(str)
df['sys_transaction_id']=df['sys_transaction_id'].astype(str)
df['id']=df['id'].astype(str)
df['id_controle_data_lake']=df['id_controle_data_lake'].astype(str)

print('inteiro')

def complex_function(x):
    is_int = False
    try: 
        y = int(x)
        return str(y)
    except ValueError:
        return str('')

def complex_function_trim(x):
    return str(x).strip()[0:16380]



df['aprovado']=   df['aprovado'].apply(complex_function)
df['assinatura_digital']=df['assinatura_digital'].apply(complex_function)
df['score']=df['score'].apply(complex_function)
df['fluxo_medio_meses']=df['fluxo_medio_meses'].apply(complex_function)
df['prazo_minimo']=df['prazo_minimo'].apply(complex_function)
df['spread_prazo']=df['spread_prazo'].apply(complex_function)
df['prazo_esperado']=df['prazo_esperado'].apply(complex_function)
df['sys_transaction_id']=df['sys_transaction_id'].apply(complex_function)
df['id']=df['id'].apply(complex_function)
df['id_controle_data_lake']=df['id_controle_data_lake'].apply(complex_function)


df['milestones']=df['milestones'].apply(complex_function_trim)



print('comecando')
df_ordened = df[['created', 'updated', 'id_proposta', 'id_contrato', 'data', 'ecid', 'credor', 'produto', 'valor_face', 'valor_presente', 'valor_credito', 'prazo', 'prazo_max', 'retencao', 'tac', 'custo_fixo', 'custo_total', 'custo_credito', 'fluxo_min', 'fluxo_max', 'fluxo_media', 'custo_trava', 'milestones', 'id_sacado', 'status', 'aprovado', 'assinatura_digital', 'tem_ecpf', 'score', 'etapa_fluxo', 'id_state', 'faturamento_minimo', 'fluxo_medio_meses', 'valor_limite', 'valor_minimo', 'prazo_minimo', 'spread_prazo', 'prazo_esperado', 'tx_retencao_rating', 'adquirentes_validas', 'faturamento_medio_sem_out', 'taxa_cap', 'operacao_id', 'tx_captalys_rating', 'id', 'fluxo_originacao', 'retencao_condicionada', 'fundo', 'rating', 'sys_commit_time', 'sys_file_date', 'sys_file_name', 'sys_operation', 'sys_commit_timestamp', 'sys_transaction_id', 'id_controle_data_lake']]
df_ordened.to_csv('novo_propostas.csv', sep='|', encoding='utf-8', index=False)
