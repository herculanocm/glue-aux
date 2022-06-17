import psycopg2 as pg
import psycopg2.extras

class PostgresFactory:
    def __init__(self, host: str, port: str, database: str, user: str, password: str) -> None:
        self.__host = host
        self.__port = port
        self.__database = database
        self.__user = user
        self.__password = password
        
    def open_conn(self):
        conn = pg.connect(user=self.__user,
                            password=self.__password,
                            host=self.__host,
                            port=self.__port,
                            database=self.__database)

        return conn

    def sql_select_to_dict(self, sql_query):
        conn = self.open_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql_query)
        df_fetchall = cur.fetchall()
        cur.close()
        conn.close()
        return df_fetchall