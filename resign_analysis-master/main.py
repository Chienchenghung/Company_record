'''
 @Author : CC Hung
 @Date : 2021-07-26
 @Description: SIT業務離職預測模型/Xgboost
 @Modified At   Modified By  Info
'''
import csv
import pandas as pd
import pickle
from functions import transform_data, topN, botN
import shap
from datetime import date
from db_operations import db_execute

# LOAD CLASS
db_operation = db_execute()

# READ DATA
print('Extracting Data from Metabase')
res = db_operation.get_data_from_metabase(3135)
df = pd.json_normalize(res).drop(columns=['is_upgrade', 'hiredate', 'leavedate', 'upgrade_date'])

# READ MODEL
model = pickle.load(open("./pickle/rf_cls_fs.pkl","rb"))

# GET COLUMNS USED IN MODEL
print('Loading Model')
with open('./result/xgb_features.csv') as csv_file:
    csv_reader = csv.reader(csv_file)
    list_of_columns = []
    for row in csv_reader:
        list_of_columns.extend(row)

# TRASFORM DATA
df_normalized = transform_data(df)

# MODEL
df_model = df_normalized[list_of_columns]

# SHAP
explainer = shap.TreeExplainer(model.best_estimator_)
shap_value = explainer.shap_values(df_model)
y_base = explainer.expected_value

# FINAL RESULT
df_final = pd.DataFrame(shap_value, columns=df_model.columns)
df_final['pred'] = model.predict(df_model)
df_final['prob_fs'] = model.predict_proba(df_model)[:,1]
df_final['sales_name'] = df_normalized['sales_name']
df_final['depttw'] = df_normalized['depttw']
df_final['day'] = df_normalized['d'] + 14
df_final['datadate'] = date.today().strftime("%Y-%m-%d")

sql_output = db_operation.pymssql_write(result=df_final, table='result')



# TOP VALUE OF EACH ROW
df_top = pd.DataFrame(shap_value, columns=df_model.columns)
top = df_top.apply(lambda row : topN(row,5), axis = 1)
bot = df_top.apply(lambda row : botN(row,(5)), axis = 1)
df_top_value = pd.concat([pd.DataFrame.from_dict(top), pd.DataFrame.from_dict(bot)], axis = 1)
df_top_value.columns = ['top', 'bot']
df_top_value[['top_first','top_second','top_third','top_fourth','top_fifth']] = pd.DataFrame(df_top_value.top.to_list(), index=df_top_value.index)
df_top_value[['bot_first','bot_second','bot_third','bot_fourth','bot_fifth']] = pd.DataFrame(df_top_value.bot.to_list(), index=df_top_value.index)
df_top_value = df_top_value.drop(columns = ['top', 'bot'])
for col in df_top_value.columns:
    df_top_value[col] = df_top_value[col].str[0]
df_top_value['sales_name'] = df_normalized['sales_name']
df_top_value['depttw'] = df_normalized['depttw']
df_top_value['day'] = df_normalized['d'] + 14
df_top_value['datadate'] = date.today().strftime("%Y-%m-%d")

sql_output = db_operation.pymssql_write(result=df_top_value, table='reasons')