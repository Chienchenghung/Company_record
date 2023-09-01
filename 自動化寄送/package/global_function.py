import win32com.client as win32
import pandas as pd
import pyodbc
import xlsxwriter


def send_email(receiver, cc, subject, body, filename):
    outlook = win32.Dispatch('outlook.application')
    mail = outlook.CreateItem(0)
    mail.To = receiver
    mail.CC = cc
    mail.Subject = subject
    mail.HTMLBody = body
    mail.Attachments.Add(filename)
    mail.Send()


def sql_from_db(stmt_file, db_name):
    fd = open(stmt_file, 'r', encoding="utf-8")
    stmt = fd.read()
    fd.close()
    # get data
    cnxn = pyodbc.connect('Driver={SQL Server};'
                          'Server=TPECOGCM2;'
                          'Database=%s;'
                          'Trusted_Connection=yes;' % db_name,
                          autocommit=True)
    try:
        df = pd.read_sql(stmt, cnxn)
        cnxn.commit()
        cnxn.close()
        return df
    except pyodbc.Error as err:
        print("Error Messages: ", err)
        cnxn.commit()
        cnxn.close()


def basic_sql_to_excel(stmt_file, db_name, file_to_create):
    df = sql_from_db(stmt_file, db_name)
    headers = [{"header": i} for i in list(df.columns)]
    workbook = xlsxwriter.Workbook(file_to_create)  # create a new workbook
    worksheet1 = workbook.add_worksheet()  # add a new worksheet
    worksheet1.add_table(0, 0, len(df), len(df.columns) - 1,
                         {"columns": headers, "data": df.values.tolist()})  # write data into the workbook
    workbook.close()
    return df
