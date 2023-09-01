# get data from SQL Server
import pandas as pd
import pyodbc
import hashlib

class get_data:
    def __init__(self, db_name, sql_file):
        self.db_name = db_name
        self.sql = sql_file

    def read_sql_file(self):
        with open(self.sql, 'r', encoding = 'utf-8') as f:
            sql = f.read()
        return sql
    
    # hash private data (email, phone)
    def sha256(self, data):
        if data is not None:
            return hashlib.sha256(data.encode('utf-8')).hexdigest()
    
    def get_data_from_sql(self):
        sql = self.read_sql_file()
        db_name = self.db_name
        cnxn = pyodbc.connect('Driver={SQL Server};'
                            'Server=TPECOGCM2;'
                            'Database=%s;'
                            'Trusted_Connection=yes;'%db_name,
                            autocommit=True)

        data = pd.read_sql(sql, cnxn)
        cnxn.close()
        print("Sha256")
        for i in range(len(data)):
            data.loc[i,'email'] = self.sha256(data.loc[i]['email'])
            data.loc[i,'phone'] = self.sha256(data.loc[i]['phone'])
            if i % 1000 == 0:
                print(i)
        return data

if __name__ == '__main__':
    get_data = get_data('USR_TBD', '20230420_google offline data.sql')
    get_data.get_data_from_sql()
    print(get_data.get_data_from_sql())

