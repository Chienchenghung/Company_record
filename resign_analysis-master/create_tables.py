import pandas as pd
import pymssql
from configparser import ConfigParser

parser = ConfigParser()
parser.read('./config/db_config.ini', encoding='utf-8')
server = parser.get('pymssql', 'server')
py_acct_user = parser.get('pymssql', 'py_acct_user')
py_acct_password = parser.get('pymssql', 'py_acct_password')
database = parser.get('pymssql', 'database')
db_name = parser.get('pymssql', 'db_name')
appname = parser.get('pymssql', 'appname')

conn = pymssql.connect(server=server,
                       user=py_acct_user,
                       password=py_acct_password,
                       database=db_name,
                       appname=appname)
cursor = conn.cursor(as_dict=True)
sql = """create table dbo.leaving_result (
	data_date DATETIME,
	sales_name varchar(100),
	depttw nvarchar(100),
	day int,
	d float,
	age float,
	brand float,
	upgrade float,
	gender float,
	location float,
	open_new float,
	dayoff_ma7 float,
	stv_ma7 float,
	dayoff float,
	dealcnt_ma14 float,
	closed_cnt_ma7 float,
	dealcnt_ma7 float,
	reserv_scrm_new float,
	scrm_new_cnt_ma14 float,
	dayoff_ma14 float,
	reserv_new_ma7 float,
	dayoff_ma3 float,
	work_hr_ma14 float,
	reserv_new_ma14 float,
	work_hr_ma3 float,
	new_cnt float,
	reserv_new float,
	new_cnt_ma14 float,
	closed_cnt_ma14 float,
	stv float,
	work_hr_ma7 float,
	reserv_closed float,
	stv_ma14 float,
	new_cnt_ma7 float,
	closed_cnt float,
	closed_cnt_ma3 float,
	work_hr float,
	dealcnt float,
	scrm_new_cnt float,
	stv_t float,
	new_cnt_t float,
	closed_cnt_t float,
	dayoff_t float,
	scrm_new_cnt_t float,
	reserv_scrm_new_t float,
	dayoff_ma3_t float,
	scrm_new_cnt_ma7_t float,
	reserv_new_ma14_t float,
	dayoff_ma14_t float,
	scrm_new_cnt_ma14_t float,
	pred int,
	prob float
)
"""
sql2= """
create table leaving_reasons (
data_date datetime,
sales_name nvarchar(100),
depttw nvarchar(100),
day int,
top_first nvarchar(100),
top_second nvarchar(100),
top_third nvarchar(100),
top_fourth nvarchar(100),
top_fifth nvarchar(100),
bot_first nvarchar(100),
bot_second nvarchar(100),
bot_third nvarchar(100),
bot_fourth nvarchar(100),
bot_fifth nvarchar(100),
) 
"""
sql3 = """
create table leaving_feature (
feature varchar(100),
reason nvarchar(100)
)
"""
sql4 = """
INSERT INTO LEAVING_FEATURE (
FEATURE, reason
)
values
('d',N'到職天數'),
('age',N'年齡'),
('brand',N'品牌'),
('upgrade',N'是否升級'),
('gender',N'性別'),
('location',N'地點'),
('open_new',N'是否開啟新名單'),
('dayoff_ma7',N'近7天休假數'),
('stv_ma7',N'近7天平均業績'),
('dayoff',N'累計休假數'),
('dealcnt_ma14',N'近14天成交數'),
('closed_cnt_ma7',N'近7天結案派發數'),
('dealcnt_ma7',N'近7天成交數'),
('reserv_scrm_new',N'社群新名單預約數'),
('scrm_new_cnt_ma14',N'近14天社群新名單派發數'),
('dayoff_ma14',N'近14天休假數'),
('reserv_new_ma7',N'近7天非社群新名單預約數'),
('dayoff_ma3',N'近3天休假數'),
('work_hr_ma14',N'近14天工時'),
('reserv_new_ma14',N'近14天非社群新名單預約數'),
('work_hr_ma3',N'近3天工時'),
('new_cnt',N'累計非社群新名單派發數'),
('reserv_new',N'累計非社群新名單預約數'),
('new_cnt_ma14',N'近14天非社群新名單派發數'),
('closed_cnt_ma14',N'近14天結案派發數'),
('stv',N'累計STV'),
('work_hr_ma7',N'近7天工時'),
('reserv_closed',N'累計結案預約數'),
('stv_ma14',N'近14天STV'),
('new_cnt_ma7',N'近7天非社群新名單派發數'),
('closed_cnt',N'累計結案派發數'),
('closed_cnt_ma3',N'近3天結案派發數'),
('work_hr',N'累計工時'),
('dealcnt',N'累計成交數'),
('scrm_new_cnt',N'累計社群新名單派發數'),
('stv_t',N'累計STV與七天前差異'),
('new_cnt_t',N'累計非社群新名單派發數與七天前差異'),
('closed_cnt_t',N'累計結案派發數與七天前差異'),
('dayoff_t',N'累計休假數與七天前差異'),
('scrm_new_cnt_t',N'累計社群新名單派發數與七天前差異'),
('reserv_scrm_new_t',N'累計社群新名單預約數與七天前差異'),
('dayoff_ma3_t',N'近3日休假數與七天前差異'),
('scrm_new_cnt_ma7_t',N'近7日社群新名單派發數與七天前差異'),
('reserv_new_ma14_t',N'近14天非社群新名單預約數與七天前差異'),
('dayoff_ma14_t',N'近14天休假數與七天前差異'),
('scrm_new_cnt_ma14_t',N'近14天社群新名單派發數與七天前差異')
"""
drop = """
drop table leaving_reasons
"""
for sql in [sql, sql2, sql3, sql4]:
    cursor.execute(sql)
    conn.commit()
conn.close()