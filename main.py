import csv
from time import perf_counter
from Connections.allConn import DBConnection
file_name = 'Network_dataset_1.csv' # enter your csv file path
table_name = 'new_table3'

class str2(str): # converting "Prakash" to 'Prakash'
     def __repr__(self):
         return ''.join(("'", super().__repr__()[1:-1], "'"))

def get_length(): # Calculates the total number of column present in csv
    with open(file_name) as csv_file:
        csv_reader = csv.DictReader(csv_file)
        header = csv_reader.fieldnames
        return len(header)
def field_conversion(): # Making fields for table
    field = "("
    with open(file_name) as csv_file:
        csv_reader = csv.DictReader(csv_file)
        header = csv_reader.fieldnames
        for i in range(len(header)-1):
            field = field + f'{header[i]},'
        field = field + f'{header[-1]})'
        return field
def create_table():# creating table in database
    temp = ""
    DBConnection.connect_db()
    with open(file_name) as csv_file:
        csv_reader = csv.DictReader(csv_file)
        header = csv_reader.fieldnames
        for i in range(0,len(header)-1):
            temp = temp + f'{header[i]} varchar(100000),'
        temp = temp + f'{header[-1]} varchar(100000)'
        DBConnection.create_table(table_name,temp)
    DBConnection.close_db()

def insert_query(table_name,header): # returning insert query for inserting the data
    field = field_conversion()
    query = f'insert into {table_name} {field} values('
    x = ','.join(['%s']*header)
    query = query + x + ')'
    return query

def insert_db_execute(): # inserting data through execute ,required (table_name, field data)
    with open(file_name,'r') as f:
        csv_reader = csv.DictReader(f)
        header = csv_reader.fieldnames
        csv_data = csv.reader(f)
        field = field_conversion()
        DBConnection.connect_db()
        start_time = perf_counter()
        for lines in csv_data: # reading rows from csv
            str_temp = ""
            for i in range(len(header)-1):
                line_temp = lines[i].replace("'","''")
                str_temp = str_temp +f"'{str2(line_temp)}'" +","
            line_temp = lines[-1].replace("'","''")
            str_temp = str_temp +f"'{str2(line_temp)}'"
            DBConnection.insert_data(table_name,field,str_temp)
        print("overall time : ",perf_counter()-start_time)
        DBConnection.close_db()

def insert_db_executemany(): # inserting data through executemany , required(query,tuple)
    with open(file_name,'r') as f:
        csv_reader = csv.DictReader(f)
        header = csv_reader.fieldnames
        csv_data = csv.reader(f)
        count  = 0
        my_tup = ()
        my_lst = list(my_tup)
        DBConnection.connect_db()
        start_time = perf_counter()
        batch_count = 0
        total_insertion_time =0
        for lines in csv_data: # reading rows from csv
            lst = []
            if count==10000:
                my_tup = tuple(my_lst)
                # insert here 
                query = insert_query(table_name,len(header))
                start = perf_counter()
                DBConnection.insert_data_executemany(query,my_tup)
                batch_count = batch_count +1
                total_insertion_time +=perf_counter()-start
                print(f"Time taken to insert Batch {batch_count} : {perf_counter()-start}") 
                my_lst = []
                count = 0
            for i in range(len(header)-1):
                line_temp = lines[i].replace("'","''")
                lst.append(str2(line_temp))
            line_temp = lines[-1].replace("'","''")
            lst.append(str2(line_temp))
            my_lst.append(tuple(lst))
            count = count +1
        # if my_lst is not empty then insert here
        if len(my_lst)!=0: # inserting remaining data
            query = insert_query(table_name,len(header))
            start = perf_counter()
            DBConnection.insert_data_executemany(query,tuple(my_lst))
            total_insertion_time +=perf_counter()-start 
            print("Total insertion time :",total_insertion_time)
            print("overall time : ",perf_counter()-start_time)
        DBConnection.close_db()

def insert_data_through_mogrify(): # inserting data through mogrify, required (table_name, field, x->(%s,%s,%s,.....), tup)
    field  = field_conversion()
    with open(file_name,'r') as f:
        csv_reader = csv.DictReader(f)
        header = csv_reader.fieldnames
        x = '('+ ','.join(['%s']*len(header)) + ')'
        csv_data = csv.reader(f)
        count  = 0
        my_tup = ()
        my_lst = list(my_tup)
        DBConnection.connect_db()
        start_time = perf_counter()
        batch_count = 0
        total_insertion_time = 0
        for lines in csv_data: # reading rows from csv         
            lst = []
            if count==10000:
                my_tup = tuple(my_lst)
                # insert here 
                start = perf_counter()
                DBConnection.insert_data_mogrify(table_name,field,x,my_tup)
                batch_count = batch_count +1
                total_insertion_time +=perf_counter()-start
                print(f"Time taken to insert Batch {batch_count} : {perf_counter()-start}")
                my_lst = []
                count = 0
            for i in range(len(header)-1):
                line_temp = lines[i].replace("'","''")
                lst.append(str2(line_temp))
            line_temp = lines[-1].replace("'","''")
            lst.append(str2(line_temp))
            my_lst.append(tuple(lst))
            count = count +1
        # if my_lst is not empty then insert here
        if len(my_lst)!=0: # inserting remaining data
            # insert here
            start = perf_counter()
            DBConnection.insert_data_mogrify(table_name,field,x,tuple(my_lst))
            total_insertion_time +=perf_counter()-start 
            print("Total insertion time :",total_insertion_time)
            print("overall time :",perf_counter()-start_time)
        DBConnection.close_db()

if __name__ =='__main__':   
    # create_table()
    # insert_db_execute()
    # insert_data_through_mogrify()
    # insert_db_executemany()
    pass
    

    
