import pandas as pd
import json 
import ast
from pandas import json_normalize

def only_dict(d):
    '''
    Convert json string representation of dictionary to a python dict
    '''
    print(d)
    print(type(d))
    e = ast.literal_eval(d)
    print(e)
    print(type(e))
    return e

def extract_detail(df):
        df2 = df.rename(columns=
            {
                "dicionario.cod": "cod"
                }, inplace=True)
        return df2

# List1 
lst = [
    ['tom', 'reacher', 25, "{'id': '012', 'nome': 'herculano0', 'detalhes': {'cod': 1, 'nome': 'ivan'}}"],
    ['krish', 'pete', 30, "{'id': '123', 'nome': 'herculano2', 'detalhes': {'cod': 1, 'nome': 'ivan'}}"],
    ['nick', 'wilson', 26, "{'id': '345', 'nome': 'herculano3', 'detalhes': {'cod': 1, 'nome': 'ivan'}}"], 
    ['juli', 'williams', 22, "{'id': '789', 'nome': 'herculano4', 'valor': 356, 'detalhes': {'cod': 1, 'nome': 'ivan'}}"]
  
    ]
    
df = pd.DataFrame(lst, columns =['FName', 'LName', 'Age', 'propostas'])

A = json_normalize(df['propostas'].apply(only_dict).tolist(), max_level=0)
B = json_normalize(A['detalhes'].tolist(), max_level=0)
print(B.head())


# df2 = pd.DataFrame(df['datateste'].tolist())
# print(df['datateste'].tolist())
#print(df2.dtypes)