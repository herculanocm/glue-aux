import pandas as pd
import json 
import ast
from pandas import json_normalize
import numpy as np
pd.set_option('display.max_columns', None)
import copy 

def only_dict(d):
    '''
    Convert json string representation of dictionary to a python dict
    '''
    try:
        e = ast.literal_eval(d)
    except:
        print('except')
        e = ast.literal_eval('{}')
    return e

def only_dict_json(d):
    '''
    Convert json string representation of dictionary to a python dict
    '''
    try:
        conv = d.replace("'", "\"")
        ret = json.loads(conv)
        print(type(ret))
    except:
        ret = {}
    return ret

def extract_detail(df):
        df2 = df.rename(columns=
            {
                "dicionario.cod": "cod"
                }, inplace=True)
        return df2

# List1 
lst = [
    ['tom', 'reacher', 25, '''{"id": 35, "name": "Comedy", "codigo": {"id": None}}'''],
    ['krish', 'pete', 30, '''{"id": 35, "name": "Comedy", "codigo": {"id": 1}}'''],
    ['nick', 'wilson', 26, '''{"id": 35, "name": "Comedy", "codigo": {"id": 1}}'''], 
    ['juli', 'williams', 22, '''{"id": 35, "name": "Comedy", "codigo": {"id": 1}}''']
  
    ]
    
df = pd.DataFrame(lst, columns =['FName', 'LName', 'Age', 'propostas'])

# A = json_normalize(df['propostas'].apply(only_dict).tolist(), max_level=0)
# B = json_normalize(A['codigo'].tolist(), max_level=0)
# print(B.head())
# df2 = json_normalize(df['propostas'].apply(only_dict_json).tolist(), max_level=0)

# print(df2.dtypes)
# print(df2.head())


df2 = pd.DataFrame(df['propostas'].apply(only_dict).tolist())
df3 = pd.DataFrame(df2['codigo'].tolist())
# print(df['datateste'].tolist())
print(df3.dtypes)
print(df3.head())