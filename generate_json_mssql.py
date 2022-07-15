import pymssql
from utils.query_utils import QueryUtils

def select_msssql(host, port, user, password, database, str_query):
    conn = pymssql.connect(host=host, port=port, user=user, password=password, database=database)
    cursor = conn.cursor(as_dict=True)
    cursor.execute(str_query)
    df_ret = cursor.fetchall()
    conn.close()
    return df_ret

if __name__ == '__main__':
    print('iniciando')

    qutils = QueryUtils()
    
    tables = [
        {'database': 'Captalys_OP', 'schema': 'carteira', 'table': 'tb_Parcela'}
    ]

    str_query = qutils.get_str_query_file('./querys/information_schema_mssql.sql')
    str_query = qutils.minify_query(str_query)

    df_types = []

    for tt in tables:
        tuple_tables = (tt['table'], f'''{tt['schema']}.{tt['table']}''')
        str_qry = (str_query % tuple_tables)
        print(str_qry)

        df_selected = select_msssql('db.captalyx.com.br', '26498', \
            'herculano_cunha', 'Cytd2cFrYzIFjaDfQyDO', 'Captalys_OP', \
            str_qry)

        for dd in df_selected:
            if 'bit' in dd['data_type']:
                dd['data_type'] = 'int'
        df_types.append({
            'obj': tt,
            'df': df_selected
        })

    print('finalizado')