import os
import sys
import inspect
import datetime 

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir) 

from package import global_function
import win32com.client as win32
import configparser

# 參數
today = str(datetime.date.today()).replace('-', '')
parser = configparser.ConfigParser()
parser.read('../config.ini', encoding = 'utf-8')


main_table_excel = parser.get('send_email', 'main_table_excel')%today

receiver = parser.get('send_email', 'receiver')
cc_receiver = parser.get('send_email', 'cc_receiver')
mail_subject = parser.get('send_email', 'mail_subject')
mail_body = parser.get('send_email', 'mail_body')

with open(mail_body, 'r', encoding='utf-8') as f:
    mail_body_html = f.read() 

outlook = win32.Dispatch('outlook.application')
mail = outlook.CreateItem(0)
mail.To = receiver
mail.CC = cc_receiver
mail.Subject = mail_subject
mail.Body = mail_body_html



attachment = main_table_excel
mail.Attachments.Add(attachment)
mail.Send()

