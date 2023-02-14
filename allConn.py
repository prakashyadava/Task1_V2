from log import *
from config import config
import psycopg2
class DBConnection:
    conn = None
    cur = None
    def __init__(self):
        pass
      
    @classmethod
    def connect_db(cls):
        params = config()
        cls.conn  = psycopg2.connect(**params)
        cls.cur = cls.conn.cursor()
        Log.log_connection(True)
        
    @classmethod
    def create_table(cls,name,table):
        cls.cur.execute(f"create table if not exists {name}(tid serial primary key,{table}) ;")
        cls.conn.commit()
        Log.log_read()
        
    @classmethod #inserting data through execute
    def insert_data(cls,name,field,data):
        cls.cur.execute(f"insert into {name} {field} values ({data});")
        cls.conn.commit()
        Log.log_insert()

    @classmethod #inserting data through executemany
    def insert_data_executemany(cls,insert_query,tup):
        cls.cur.executemany(f"{insert_query}", tup)
        cls.conn.commit()
        Log.log_insert()
        
    @classmethod #inserting data through mogrify
    def insert_data_mogrify(cls,table_name,field,x,tup):
        mogrify_values = ','.join(cls.cur.mogrify(x,i).decode('utf-8') for i in tup)
        cls.cur.execute(f"insert into {table_name} {field} values {mogrify_values}")
        cls.conn.commit()
        Log.log_insert()
        
    @classmethod
    def close_db(cls):
        cls.conn.close()
        Log.log_close(True)
