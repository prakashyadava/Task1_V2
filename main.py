import csv
from time import perf_counter
from Connections.allConn import DBConnection

class str2(str): # converting "Prakash" to 'Prakash'
     def __repr__(self):
         return ''.join(("'", super().__repr__()[1:-1], "'"))

def read_csv():
    csv_file = open(file_name)
    csv_reader = csv.DictReader(csv_file)
    header = csv_reader.fieldnames
    csv_data = csv.reader(csv_file)
    return csv_data,header
def field_conversion(): # Making fields for table
    field = "("
    with open(file_name) as csv_file:
        csv_reader = csv.DictReader(csv_file)
        header = csv_reader.fieldnames
        for i in range(len(header)-1):
            field = field + f'{header[i]},'
        field = field + f'{header[-1]})'
        return field
def create_table(db):# creating table in database
    temp = ""
    flag = db.connect_db()
    if flag:
        with open(file_name) as csv_file:
            csv_reader = csv.DictReader(csv_file)
            header = csv_reader.fieldnames
            for i in range(0,len(header)-1):
                temp = temp + f'{header[i]} varchar(100000),'
            temp = temp + f'{header[-1]} varchar(100000)'
            check = db.create_table(table_name,temp)
            if check:
                print(f'{table_name} table is created')
                db.close_db()
                return True
            else:
                print(f'Unable to create {table_name} table')
                return False   
    else:
        print("Unable to connect the database")
        return False

def insert_query(table_name,header): # returning insert query for inserting the data
    field = field_conversion()
    query = f'insert into {table_name} {field} values('
    x = ','.join(['%s']*header)
    query = query + x + ')'
    return query

def insert_db_execute(db): # inserting data through execute ,required (table_name, field data)
    csv_data,header = read_csv()
    field = field_conversion()
    flag = db.connect_db()
    if flag:
        start_time = perf_counter()
        for lines in csv_data: # reading rows from csv
            str_temp = ""
            for i in range(len(header)-1):
                line_temp = lines[i].replace("'","''")
                str_temp = str_temp +f"'{str2(line_temp)}'" +","
            line_temp = lines[-1].replace("'","''")
            str_temp = str_temp +f"'{str2(line_temp)}'"
            db.insert_data(table_name,field,str_temp)
        print("overall time : ",perf_counter()-start_time)
        db.close_db()
    else:
        print("Unable to connect the database")

def insert_db_executemany(db): # inserting data through executemany , required(query,tuple)
    csv_data,header = read_csv()
    count  = 0
    my_tup = ()
    my_lst = list(my_tup)
    flag = db.connect_db()
    if flag:
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
                db.insert_data_executemany(query,my_tup)
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
            db.insert_data_executemany(query,tuple(my_lst))
            total_insertion_time +=perf_counter()-start 
            print("Total insertion time :",total_insertion_time)
            print("overall time : ",perf_counter()-start_time)
        db.close_db()
    else:
        print("Unable to connect the Database")

def insert_data_through_mogrify(db): # inserting data through mogrify, required (table_name, field, x->(%s,%s,%s,.....), tup)
    
    field  = field_conversion()
    csv_data,header = read_csv()
    x = '('+ ','.join(['%s']*len(header)) + ')'
    count  = 0
    my_tup = ()
    my_lst = list(my_tup)
    flag = db.connect_db()
    if flag:
        start_time = perf_counter()
        batch_count = 0
        total_insertion_time = 0
        for lines in csv_data: # reading rows from csv         
            lst = []
            if count==10000:
                my_tup = tuple(my_lst)
                # insert here 
                start = perf_counter()
                db.insert_data_mogrify(table_name,field,x,my_tup)
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
            db.insert_data_mogrify(table_name,field,x,tuple(my_lst))
            total_insertion_time +=perf_counter()-start 
            print("Total insertion time :",total_insertion_time)
            print("overall time :",perf_counter()-start_time)
        db.close_db()
    else:
        print("Unable to connect the Database")

if __name__ =='__main__':
    db = DBConnection()
    file_name = '/Users/prakashyadava/Desktop/Task1_V2/Data/Network_dataset_1.csv' # enter your csv file path
    table_name = 'new_table3' 
    table_created = False 
    try:   
        table_created= create_table(db)
    except Exception as e:
        print(e)
    try:
        if table_created:
            insert_data_through_mogrify(db)
    except Exception as e:
        print(e)
    # insert_db_executemany(db)
    # insert_db_execute(db)
    
    pass
    

    
