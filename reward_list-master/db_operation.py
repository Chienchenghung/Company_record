#!/usr/bin/env python 

import pandas as pd
import pymssql
from datetime import datetime as dt
from configparser import ConfigParser

class db_execute:
    def __init__(self):
        parser = ConfigParser()
        parser.read('db_config.ini', encoding = 'utf-8')   
        self.server_cm2 = parser.get('CM2', 'server')
        self.py_acct_user_cm2 = parser.get('CM2', 'py_acct_user')
        self.py_acct_password_cm2 = parser.get('CM2', 'py_acct_password')
        self.db_name_cm2 = parser.get('CM2', 'db_name')
        self.appname_cm2 = parser.get('CM2', 'appname')      
        
    def pymssql_sp_cm2(self, proc):
        conn = pymssql.connect(server=self.server_cm2,
                               user=self.py_acct_user_cm2,
                               password=self.py_acct_password_cm2, 
                               database=self.db_name_cm2,
                               appname=self.appname_cm2,
                               autocommit=True)    
        cursor = conn.cursor(as_dict=True)
        cursor.execute(proc)
        df = pd.DataFrame(cursor.fetchall())
        conn.commit()
        conn.close()
        return(df)
    
    def pymssql_sp_cm2_exec(self, proc):
        conn = pymssql.connect(server=self.server_cm2,
                               user=self.py_acct_user_cm2,
                               password=self.py_acct_password_cm2, 
                               database=self.db_name_cm2,
                               appname=self.appname_cm2,
                               autocommit=True)    
        cursor = conn.cursor(as_dict=True)
        cursor.execute(proc)
        conn.commit()
        conn.close()
        
    def pymssql_read(self, sql):
        conn = pymssql.connect(server=self.server_cm2,
                               user=self.py_acct_user_cm2,
                               password=self.py_acct_password_cm2, 
                               database=self.db_name_cm2,
                               appname=self.appname_cm2)    
        cursor = conn.cursor(as_dict=True)
        cursor.execute(sql)
        df = pd.DataFrame(cursor.fetchall())
        conn.close()
        return(df)
        
    def pymssql_write(self, result):
        conn = pymssql.connect(server=self.server_cm2,
                               user=self.py_acct_user_cm2,
                               password=self.py_acct_password_cm2, 
                               database=self.db_name_cm2,
                               appname=self.appname_cm2)    
        cursor = conn.cursor(as_dict=True)
        
        data_date = result.iloc[0,0]
        sql_query = f"""DELETE FROM dbo.AwardList WHERE DataDate='{data_date}'\n\n"""
        for i in range(result.shape[0]):
            row = result.iloc[i]
            sql = f"""INSERT INTO dbo.AwardList (DataDate, contractsn, pred_prob1, reason1, reason2, reason3, ETL_DATE) 
                    VALUES('{row.DataDate}', {row.contractsn}, {row.pred_prob1}, N'{row.reason1}', N'{row.reason2}',
                    N'{row.reason3}', '{row.ETL_DATE}')\n\n"""
            sql_query = sql_query + sql
            if(i % 50000 == 0):
                cursor.execute(sql_query)        
                conn.commit()
                cursor.close()
                conn.close()
                conn = pymssql.connect(server=self.server_cm2,
                       user=self.py_acct_user_cm2,
                       password=self.py_acct_password_cm2, 
                       database=self.db_name_cm2,
                       appname=self.appname_cm2)    
                cursor = conn.cursor(as_dict=True)
                sql_query = f"""\n\n"""
        
        cursor.execute(sql_query)        
        conn.commit()
        cursor.close()
        conn.close()
        return(sql_query)
        
    def pymssql_write_sql_file(self, result):
        data_date = result.iloc[0,0]
        sql_query = f"""DELETE FROM dbo.AwardList WHERE DataDate='{data_date}'\n\n"""
        for i in range(result.shape[0]):
            row = result.iloc[i]
            sql = f"""INSERT INTO dbo.AwardList (DataDate, contractsn, pred_prob1, reason1, reason2, reason3, ETL_DATE) 
                    VALUES('{row.DataDate}', {row.contractsn}, {row.pred_prob1}, N'{row.reason1}', N'{row.reason2}',
                    N'{row.reason3}', '{row.ETL_DATE}')\n\n"""
            sql_query = sql_query + sql
        return(sql_query)
