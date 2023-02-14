from LOGS.log import Log
from .config import config
import psycopg2
class DBConnection:
    conn = None
    cur = None
    _instance = None

    def __new__(cls):
        if not cls._instance:
            cls._instance = super(DBConnection,cls).__new__(cls)
        return cls._instance
    
    def connect_db(self):
        try:
            params = config()
            self.conn  = psycopg2.connect(**params)
            self.cur = self.conn.cursor()
            Log.log_connection(True)
            return True
        except psycopg2.Error as e:
            print(f'{e}')
            return False
    
    def create_table(self,name,table):
        try:
            self.cur.execute(f"create table if not exists {name}(tid serial primary key,{table}) ;")
            self.conn.commit()
            Log.log_read()
            return True
        except psycopg2.Error as e:
            print(f'{e}')
            return False
    def display_records(self,name):
        try:
            self.cur.execute(f"select * from {name} ;")
            data  =self.cur.fetchall()
            return data
        except Exception as e:
            print(e)

    #inserting data through execute
    def insert_data(self,name,field,data):
        try:
            self.cur.execute(f"insert into {name} {field} values ({data});")
            self.conn.commit()
            Log.log_insert()
        except psycopg2.Error as e:
            print(f'{e}')

    #inserting data through executemany
    def insert_data_executemany(self,insert_query,tup):
        try:
            self.cur.executemany(f"{insert_query}", tup)
            self.conn.commit()
            Log.log_insert()
        except psycopg2.Error as e:
            print(f'{e}')
    #inserting data through mogrify
    def insert_data_mogrify(self,table_name,field,x,tup):
        try:
            mogrify_values = ','.join(self.cur.mogrify(x,i).decode('utf-8') for i in tup)
            self.cur.execute(f"insert into {table_name} {field} values {mogrify_values}")
            self.conn.commit()
            Log.log_insert()
        except psycopg2.Error as e:
            print(f'{e}')
    def close_db(self):
        try:
            self.conn.close()
            Log.log_close(True)
        except psycopg2.Error as e:
            print(f'{e}')
