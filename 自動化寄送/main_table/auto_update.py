import os
import sys
import inspect
import datetime 

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir) 

# from package import global_function

import pandas as pd
import pyodbc
import configparser

# 參數
today = str(datetime.date.today()).replace('-', '')
parser = configparser.ConfigParser()
parser.read('../config.ini', encoding = 'utf-8')


def get_data_from_sql(sql, db_name):
    cnxn = pyodbc.connect('Driver={SQL Server};'
                        'Server=TPECOGCM2;'
                        'Database=%s;'
                        'Trusted_Connection=yes;'%db_name,
                        autocommit=True)

    data = pd.read_sql(sql, cnxn)
    cnxn.close()
    return data


# 讀取sql檔

with open('./20230420_google offline data.sql', 'r', encoding = 'utf-8') as f:
    new_sql = f.read()

print('sql讀取完成')
# 更新數據

## 新名單
new_data = get_data_from_sql(new_sql, 'USR_TBD')


print('連線數據更新完成')
## 主表更新
