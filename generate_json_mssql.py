import pymssql

def select_msssql(host, port, user, password, database, str_query):
    conn = pymssql.connect(host=host, port=port, user=user, password=password, database=database)
    cursor = conn.cursor(as_dict=True)
    cursor.execute(str_query)
    df_ret = cursor.fetchall()
    conn.close()
    return df_ret

if __name__ == '__main__':
    print('iniciando')
    print(select_msssql('db.captalyx.com.br', '26498', 'herculano_cunha', 'Cytd2cFrYzIFjaDfQyDO', 'Captalys_OP', 'SELECT top 10 * FROM Captalys_OP.carteira.tb_Ativo'))
    print('finalizado')