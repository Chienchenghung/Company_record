import pandas as pd
import pymssql
from configparser import ConfigParser
from datetime import date
from metabase_api import Metabase_API


class db_execute:
    def __init__(self):
        parser = ConfigParser()
        parser.read('./config/db_config.ini', encoding='utf-8')
        self.server = parser.get('pymssql', 'server')
        self.py_acct_user = parser.get('pymssql', 'py_acct_user')
        self.py_acct_password = parser.get('pymssql', 'py_acct_password')
        self.database = parser.get('pymssql', 'database')
        self.db_name = parser.get('pymssql', 'db_name')
        self.appname = parser.get('pymssql', 'appname')
        self.url = parser.get('metabase', 'url')
        self.mb_acct = parser.get('metabase', 'mb_acct')
        self.mb_pwd = parser.get('metabase', 'mb_pwd')



    def pymssql_read(self, sql):
        conn = pymssql.connect(server=self.server,
                               user=self.py_acct_user,
                               password=self.py_acct_password,
                               database=self.db_name,
                               appname=self.appname)
        cursor = conn.cursor(as_dict=True)
        cursor.execute(sql)
        df = pd.DataFrame(cursor.fetchall())
        conn.close()
        return (df)

    def pymssql_write(self, result, table = "result"):
        conn = pymssql.connect(server=self.server,
                               user=self.py_acct_user,
                               password=self.py_acct_password,
                               database=self.db_name,
                               appname=self.appname)
        cursor = conn.cursor(as_dict=True)
        data_date = date.today().strftime("%Y-%m-%d")
        if table == "result":
            print('writing data to leaving_result')
            sql_query = f"""DELETE FROM {self.db_name}.dbo.leaving_result WHERE Data_Date='{data_date}'\n\n"""
            for i in range(result.shape[0]):
                row = result.iloc[i]
                sql = f"""INSERT INTO {self.db_name}.dbo.leaving_result(data_date,
            sales_name,
            depttw,
            day,
            d,
            age,
            brand,
            upgrade,
            gender,
            location,
            open_new,
            dayoff_ma7,
            stv_ma7,
            dayoff,
            dealcnt_ma14,
            closed_cnt_ma7,
            dealcnt_ma7,
            reserv_scrm_new,
            scrm_new_cnt_ma14,
            dayoff_ma14,
            reserv_new_ma7,
            dayoff_ma3,
            work_hr_ma14,
            reserv_new_ma14,
            work_hr_ma3,
            new_cnt,
            reserv_new,
            new_cnt_ma14,
            closed_cnt_ma14,
            stv,
            work_hr_ma7,
            reserv_closed,
            stv_ma14,
            new_cnt_ma7,
            closed_cnt,
            closed_cnt_ma3,
            work_hr,
            dealcnt,
            scrm_new_cnt,
            stv_t,
            new_cnt_t,
            closed_cnt_t,
            dayoff_t,
            scrm_new_cnt_t,
            reserv_scrm_new_t,
            dayoff_ma3_t,
            scrm_new_cnt_ma7_t,
            reserv_new_ma14_t,
            dayoff_ma14_t,
            scrm_new_cnt_ma14_t,
            pred,
            prob
            )
            VALUES ('{row.datadate}',
            '{row.sales_name}',
            N'{row.depttw}',
            {row.day},
            {row.d},
            {row.age},
            {row.brand},
            {row.upgrade},
            {row.gender},
            {row.location},
            {row.open_new},
            {row.dayoff_ma7},
            {row.stv_ma7},
            {row.dayoff},
            {row.dealcnt_ma14},
            {row.closed_cnt_ma7},
            {row.dealcnt_ma7},
            {row.reserv_scrm_new},
            {row.scrm_new_cnt_ma14},
            {row.dayoff_ma14},
            {row.reserv_new_ma7},
            {row.dayoff_ma3},
            {row.work_hr_ma14},
            {row.reserv_new_ma14},
            {row.work_hr_ma3},
            {row.new_cnt},
            {row.reserv_new},
            {row.new_cnt_ma14},
            {row.closed_cnt_ma14},
            {row.stv},
            {row.work_hr_ma7},
            {row.reserv_closed},
            {row.stv_ma14},
            {row.new_cnt_ma7},
            {row.closed_cnt},
            {row.closed_cnt_ma3},
            {row.work_hr},
            {row.dealcnt},
            {row.scrm_new_cnt},
            {row.stv_t},
            {row.new_cnt_t},
            {row.closed_cnt_t},
            {row.dayoff_t},
            {row.scrm_new_cnt_t},
            {row.reserv_scrm_new_t},
            {row.dayoff_ma3_t},
            {row.scrm_new_cnt_ma7_t},
            {row.reserv_new_ma14_t},
            {row.dayoff_ma14_t},
            {row.scrm_new_cnt_ma14_t},
            {row.pred},
            {row.prob_fs}
            )\n\n"""
                sql_query = sql_query + sql
        else:
            print('writing data to leaving_reasons')
            sql_query = f"""DELETE FROM {self.db_name}.dbo.leaving_reasons WHERE Data_Date='{data_date}'\n\n"""
            for i in range(result.shape[0]):
                row = result.iloc[i]
                sql = f"""INSERT INTO {self.db_name}.dbo.leaving_reasons(
                data_date,
                top_first,
                top_second,
                top_third,
                top_fourth,
                top_fifth,
                bot_first,
                bot_second,
                bot_third,
                bot_fourth,
                bot_fifth,
                sales_name,
                depttw,
                day
                )
                VALUES (
                '{row.datadate}',
                N'{row.top_first}',
                N'{row.top_second}',
                N'{row.top_third}',
                N'{row.top_fourth}',
                N'{row.top_fifth}',
                N'{row.bot_first}',
                N'{row.bot_second}',
                N'{row.bot_third}',
                N'{row.bot_fourth}',
                N'{row.bot_fifth}',
                N'{row.sales_name}',
                N'{row.depttw}',
                {row.day}
                )\n\n"""
                sql_query = sql_query + sql
        sql_query = sql_query.replace("nan", "NULL")
        cursor.execute(sql_query)
        conn.commit()
        conn.close()

    def get_data_from_metabase(self, q_id):
        mb = Metabase_API(self.url, self.mb_acct, self.mb_pwd)
        res = mb.post("/api/card/{}/query/{}".format(q_id, 'json'))
        return res
