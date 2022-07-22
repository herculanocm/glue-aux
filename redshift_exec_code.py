from datetime import date, timedelta, datetime
import pg8000 as pg

def exec_query_with_commit(str_qry):
   
    conn = pg.connect(
        database='datalake_dw',
        host='asgard-redshift-production.cmqegk5gj3mi.sa-east-1.redshift.amazonaws.com',
        port='5439',
        user='app_analytics_owner',
        password='4G2U37AQ4ywACO0bDJzW'
    )
    cur = conn.cursor()
    
    cur.execute(str_qry)

                
    cur.close()
    cur.close()

start_date = date(2022, 6, 6) 
end_date = date(2022, 7, 21)    # perhaps date.now()

delta = end_date - start_date   # returns timedelta

for i in range(delta.days + 1):
    inicio_time = datetime.now()
    day = start_date + timedelta(days=i)
    str_command = f'''call captalys_analytics.sp_iud_estoque_diario_adm('{day}')'''
    print(str_command)
    exec_query_with_commit(str_command)
    print(f'''finalizado dia {day}''')
    final_time = datetime.now()
    difference = inicio_time - final_time
    seconds_in_day = 24 * 60 * 60
    ss = divmod(difference.days * seconds_in_day + difference.seconds, 60)
    print(f'''minutes and seconds:  {ss}''')