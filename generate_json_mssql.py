import pymssql
from utils.query_utils import QueryUtils
import json
import sys

def select_msssql(host, port, user, password, database, str_query):
    conn = pymssql.connect(host=host, port=port, user=user, password=password, database=database)
    cursor = conn.cursor(as_dict=True)
    cursor.execute(str_query)
    df_ret = cursor.fetchall()
    conn.close()
    return df_ret

if __name__ == '__main__':


    qutils = QueryUtils()
    
    tables = [
        {'database': 'Captalys_OP', 'schema': 'carteira', 'table': 'Justificativa', 'alias': 'tee'},
        {'database': 'Captalys_OP', 'schema': 'carteira', 'table': 'Justificativa', 'alias': 'tee'},
        {'database': 'Captalys_OP', 'schema': 'carteira', 'table': 'tb_Amortizacao', 'alias': 'tee'},
        {'database': 'Captalys_OP', 'schema': 'carteira', 'table': 'tb_BTGCarteira', 'alias': 'tee'},
        {'database': 'Captalys_OP', 'schema': 'carteira', 'table': 'tb_BrlTrust', 'alias': 'tee'},
        {'database': 'Captalys_OP', 'schema': 'carteira', 'table': 'tb_CPR', 'alias': 'tee'},
        {'database': 'Captalys_OP', 'schema': 'carteira', 'table': 'tb_CaixaConsolidado', 'alias': 'tee'},
        {'database': 'Captalys_OP', 'schema': 'carteira', 'table': 'tb_CategoriaAtivo', 'alias': 'tee'},
        {'database': 'Captalys_OP', 'schema': 'carteira', 'table': 'tb_ContasCorrentes', 'alias': 'tee'},
        {'database': 'Captalys_OP', 'schema': 'carteira', 'table': 'tb_Derivativos', 'alias': 'tee'},
        {'database': 'Captalys_OP', 'schema': 'carteira', 'table': 'tb_FundoDeInvestimentos', 'alias': 'tee'},
        {'database': 'Captalys_OP', 'schema': 'carteira', 'table': 'tb_FundoFeeder', 'alias': 'tee'},
        {'database': 'Captalys_OP', 'schema': 'carteira', 'table': 'tb_FundoFidc', 'alias': 'tee'},
        {'database': 'Captalys_OP', 'schema': 'carteira', 'table': 'tb_GrupoEconomico', 'alias': 'tee'},
        {'database': 'Captalys_OP', 'schema': 'carteira', 'table': 'tb_InstrumentoFinanceiro', 'alias': 'tee'},
        {'database': 'Captalys_OP', 'schema': 'carteira', 'table': 'tb_InstrumentoFinanceiroAditado', 'alias': 'tee'},
        {'database': 'Captalys_OP', 'schema': 'carteira', 'table': 'tb_NPLFidcNp', 'alias': 'tee'},
        {'database': 'Captalys_OP', 'schema': 'carteira', 'table': 'tb_OutputPLFundo', 'alias': 'tee'},
        {'database': 'Captalys_OP', 'schema': 'carteira', 'table': 'tb_OutrosAtivos', 'alias': 'tee'},
        {'database': 'Captalys_OP', 'schema': 'carteira', 'table': 'tb_PDD', 'alias': 'tee'},
        {'database': 'Captalys_OP', 'schema': 'carteira', 'table': 'tb_ParcelaAditado', 'alias': 'tee'},
        {'database': 'Captalys_OP', 'schema': 'carteira', 'table': 'tb_ParcelaFluxoOriginal', 'alias': 'tee'},
        {'database': 'Captalys_OP', 'schema': 'carteira', 'table': 'tb_RendaFixa', 'alias': 'tee'},
        {'database': 'Captalys_OP', 'schema': 'carteira', 'table': 'tb_RendaVariavel', 'alias': 'tee'},
        {'database': 'Captalys_OP', 'schema': 'carteira', 'table': 'tb_Rent', 'alias': 'tee'},
        {'database': 'Captalys_OP', 'schema': 'carteira', 'table': 'tb_Rentabilidade', 'alias': 'tee'},
        {'database': 'Captalys_OP', 'schema': 'carteira', 'table': 'tb_RetencaoParcelaVariavel', 'alias': 'tee'},
        {'database': 'Captalys_OP', 'schema': 'carteira', 'table': 'tb_SocopaCarteira', 'alias': 'tee'},
        {'database': 'Captalys_OP', 'schema': 'carteira', 'table': 'tb_SocopaCarteiraPiloto', 'alias': 'tee'},
        {'database': 'Captalys_OP', 'schema': 'carteira', 'table': 'tb_Taxa', 'alias': 'tee'},
        {'database': 'Captalys_OP', 'schema': 'carteira', 'table': 'tb_Tesouraria', 'alias': 'tee'},
        {'database': 'Captalys_OP', 'schema': 'carteira', 'table': 'tb_limite', 'alias': 't1'}
    ]

    str_query = qutils.get_str_query_file('./querys/information_schema_mssql.sql')
    str_query = qutils.minify_query(str_query)

    df_types = []

    for tt in tables:
        tuple_tables = (tt['table'], f'''{tt['schema']}.{tt['table']}''')
        str_qry = (str_query % tuple_tables)


        df_selected = select_msssql('db.captalyx.com.br', '26498', \
            'herculano_cunha', 'Cytd2cFrYzIFjaDfQyDO', 'Captalys_OP', \
            str_qry)

        pks = []
        for dff in df_selected:
            print(dff)
            if dff['primary_key'] == True:
                pks.append(dff['column_name'])


        for pk in pks:
            for i in range(len(df_selected)):
                if df_selected[i]['column_name'] == pk and df_selected[i]['primary_key'] == False:
                    del df_selected[i]
                    break



        lst_columns = []
        for dd in df_selected:
            dd['row_num'] = False
            dd['column_where'] = False
            dd['alias'] = tt['alias']
            dd['column_operation'] = False
            lst_columns.append(f"{tt['alias']}.{dd['column_name']}")
            if 'bit' in dd['data_type']:
                dd['data_type'] = 'int'




        df_types.append({
            'obj': tt,
            'table': tt['table'],
            'df': df_selected,
            'query_full': " select " + (", ".join(lst_columns)) + " from " + f"{tt['schema']}.{tt['table']} {tt['alias']} with(nolock) "
        })

dicts_json = []
for jj in df_types:
    obj = {
        'table': jj['table'],
        'query_full': jj['query_full'],
        'query_delta': '',
        'columns': jj['df']
    }
    dicts_json.append(obj)

for ep in dicts_json:
    json_export = json.dumps(ep, indent=4)
    
    schema_file = open(f"./json/{ep['table'].lower()}.json", "w")
    schema_file.write(json_export)
    schema_file.close()