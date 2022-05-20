import pandas as pd
df = pd.read_parquet('/home/herculano/download/parcela/part.parquet', engine='pyarrow')

print(df.info(verbose=True))