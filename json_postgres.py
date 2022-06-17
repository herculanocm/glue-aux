from utils.query_utils import QueryUtils
from utils.postg_utils import PostgresFactory
import json 
import datetime
import calendar

def default(o):
    if isinstance(o, (datetime.datetime)):
        # o.isoformat()
        return calendar.timegm(o.utctimetuple()) 

utils = QueryUtils()
postg_utils = PostgresFactory(
    user="herculano_cunha",
    password="ks9o0QJou3kyLRZswXJd",
    host="asgard-postgresql-production-replica.captalysplatform.io",
    port="35432",
    database="pricing_clj"
)

print('buscando')
result_query = postg_utils.sql_select_to_dict("select * from master.offer")

#print(result_query)
print('json dumps')
result_query2= json.dumps( result_query,   sort_keys=True,      default=default)
print('json loads')
result_query3 = json.loads(result_query2)

result4 = []
for it in result_query3:
    obj = {
        'before': None,
        'after': it,
        "source": {
        "version":"1.5.0.Final",
        "connector":"postgresql",
        "name":"pricing_clj",
        "ts_ms":1,
        "snapshot":"false",
        "db":"pricing_clj",
        "sequence":"[]",
        "schema":"master",
        "table":"offer",
        "txId":1,
        "lsn":1,
        "xmin": None
        },
    "op":"c",
    "ts_ms":1655263078938,
    "transaction": None
    }
    result4.append(obj)

print('ultimo dumps')
result5 = json.dumps( result4)
schema_file = open(f"./json/1.json", "w")
schema_file.write(result5)
schema_file.close()

print('terminado')
