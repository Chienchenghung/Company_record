# get data from SQL Server
import pandas as pd
import pyodbc
import hashlib
import datetime

class get_data:
    today = datetime.datetime.now().strftime("%Y%m%d")
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
        if 'email' in data.columns or 'phone' in data.columns:
            for i in range(len(data)):
                data.loc[i,'email'] = self.sha256(data.loc[i]['email'])
                data.loc[i,'phone'] = self.sha256(data.loc[i]['phone'])
                if i % 1000 == 0:
                    print(i)
        else :
            print("No email or phone in data")
        return data

if __name__ == '__main__':
    path = '\\\\192.168.70.6\\BI_MD_Share\\2023_google offline data\\'
    get_data = get_data('USR_TBD', '20230420_Meta offline data.sql')
    get_data.get_data_from_sql().to_csv(f'{path}{get_data.today}_Meta offline data.csv', index=False)
