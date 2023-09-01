# get data from SQL Server
import pandas as pd
from pandas import DataFrame
from sqlalchemy import create_engine, text
from iTGConnCrypt import iTGConnCrypt as ConnCrypt


class get_data:
    def __init__(self, db_name, sql_file):
        self.db_name = db_name
        self.sql = sql_file

    def read_sql_file(self):
        with open(self.sql, 'r', encoding='utf-8') as f:
            sql = f.read()
        return sql
    
    # hash private data (email, phone)
    """def sha256(self, data):
        if data is not None:
            return hashlib.sha256(data.encode('utf-8')).hexdigest()"""
    def getConn(self):
        con = ConnCrypt()
        connStr = con.getDecConn()
        return connStr

    def get_data_from_sql(self):
        sql = self.read_sql_file()
        self.connStr = self.getConn()
        self.strConn = f"mssql+pymssql://{self.connStr}/{self.db_name}"
        engine = create_engine(self.strConn)
        with engine.begin() as conn:
            data=conn.execute(text(sql))
            df= DataFrame(data)
            return df
        #cnxn.close()
        """print("Sha256")
        for i in range(len(data)):
            data.loc[i,'email'] = self.sha256(data.loc[i]['email'])
            data.loc[i,'phone'] = self.sha256(data.loc[i]['phone'])
            if i % 1000 == 0:
                print(i)"""
        

if __name__ == '__main__':
    get_data1 = get_data('BI_EDW', '20230606_media target.sql')
    data= get_data1.get_data_from_sql()
    data