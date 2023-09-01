#!/usr/bin/env python 

from sklearn.preprocessing import StandardScaler
from lightgbm import LGBMClassifier
from datetime import date, timedelta, datetime

import shap
import pandas as pd
import numpy as np
import joblib
import logging
import warnings

from db_operation import db_execute

db_operation = db_execute()

warnings.filterwarnings("ignore")
FORMAT = '%(asctime)s %(levelname)s: %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT)

dat = datetime.strftime(date.today(),"%Y-%m-%d")

# 輸入資料

logging.info('Read Data From Database')

file = 'query.sql'

with open(file, 'r') as fd:
 	sql = fd.read()

df = db_operation.pymssql_sp_cm2(sql)

df["end_contract_time"] = pd.to_datetime(df["end_contract_time"])
       
# 切分解釋變數以及被解釋變數

Xi, y = df.iloc[:,1:-2], df[["continue_sign_before", 'end_contract_time']]

cat = ["Pay_Mode", "Team_Function", "client_JobClassName", "L1_ENAME", "client_nlevel"]
X_cat = Xi[cat]
X_num = Xi.drop(cat, axis=1)

# 數據標準化

logging.info('Normalizing Data')

X_cat = pd.get_dummies(X_cat, columns=cat)
X_num = pd.DataFrame(StandardScaler().fit_transform(X_num), index=X_num.index, columns=X_num.columns)

X = pd.concat([X_num, X_cat], axis=1)
X['end_contract_time'] = df['end_contract_time']
X = X[X.end_contract_time > dat]
X = X.drop(['end_contract_time'], axis = 1)

y = y[y.end_contract_time > dat]
y = y.drop(['end_contract_time'], axis = 1)

logging.info('Predicting Data with size ' + str(X.shape))

logging.info('Start Predicting Data')

clf = joblib.load('award_list_gbm.pkl')

y_pred = clf.predict(X)

prob = pd.DataFrame(clf.predict_proba(X), columns = ['pred_prob0', 'pred_prob1'])
prob = prob.drop("pred_prob0", axis = 1)
prob.pred_prob1 = prob.pred_prob1.round(3)

logging.info('End Predicting Data')

logging.info('Start Analyzing Data')

explainer = shap.TreeExplainer(clf)
reason1 = []; reason2 = []; reason3 = [];

mapping = {
    'willing_attend_evening' : '願意在半夜上課',
    'TotalContractPoints' : '合約堂數少',
    'BI_STV' : 'BI_STV高',
    'freegifts' : '贈送堂數多',
    'client_JobClassName_Student' : '職業為學生',
    'client_JobClassName_Professional' : '職業為專業人員',
    'client_JobClassName_Engineer' : '職業為工程師',
    'client_JobClassName_Teacher' : '職業為老師',
    'client_JobClassName_Management' : '職業為管理階級',
    'client_JobClassName_Executive' : '職業為負責人／股東/政治人物',
    'client_JobClassName_General Staff' : '職業為一般行政、公務人員',
    'RefundAnnounce' : '沒有發過退費通報',
    'Team_Function_Normal' : '團隊功能為普通團隊',
    'Team_Function_continue_sign' : '團隊功能為續約團隊',
    'client_nlevel_11.0' : '高等級客戶',
    'client_nlevel_12.0' : '高等級客戶',
    'client_nlevel_10.0' : '高等級客戶',
    'client_nlevel_1.0' : '低等級客戶',
    'client_nlevel_2.0' : '低等級客戶',
    'client_nlevel_3.0' : '低等級客戶',
    'avg_cpoint' : '平均顧問評鑑分數高',
    'avg_mpoint' : '平均教材評鑑分數高',
    'avg_tpoint' : '平均通訊評鑑分數高',
    'avg_con_complain' : '顧問抱怨少',
    'avg_mat_complain' : '平均教材抱怨數少',
    'avg_tec_complain' : '平均技術抱怨數少',
    'avg_con_compliment' : '平均顧問讚美數多',
    'avg_mat_compliment' : '平均教材讚美數多',
    'Pay_Mode_OnlineLoan' : '線上貸款',
    'Pay_Mode_OfflineLoan' : '線下貸款',
    'Pay_Mode_Credit_Card' : '信用卡分期',
    'Pay_Mode_PayOneTime' : '一次付清',
    'L1_ENAME_Official Website' : '第一層媒體為Official Website',
    'L1_ENAME_Exhibition' : '大型展會',
    'L1_ENAME_Speech/Activity' : '第一層媒體為講座和實體活動',
    'L1_ENAME_Offline Promotion' : '第一層媒體為地推活動',
    'L1_ENAME_From_TutorJr' : '第一層媒體為Jr轉入',
    'L1_ENAME_Internet' : '第一層媒體為網路廣告-其他',
    'L1_ENAME_Relatives/Friends' : '第一層媒體為轉介紹',
    'L1_ENAME_Client Referral' : '第一層媒體為客戶介紹',
    'L1_ENAME_T-Space Promotion' : '第一層媒體為T館活動',
    'L1_ENAME_Yahoo' : '第一層媒體為網路廣告-Yahoo',
    'L1_ENAME_Radio/TV' : '第一層媒體為廣播',
    'L1_ENAME_Magazine' : '第一層媒體為雜誌廣告',
    'L1_ENAME_MSN' : '第一層媒體為網路廣告-MSN',
    'L1_ENAME_Others' : '第一層媒體為其他媒體',
    'L1_ENAME_DM' : '第一層媒體為傳單',
    'L1_ENAME_Bus Media' : '第一層媒體為戶外/公車廣告',
    'L1_ENAME_Board' : '第一層媒體為看板廣告',
    'L1_ENAME_Cold Call' : '第一層媒體為陌生拜訪',
    'L1_ENAME_Google' : '第一層媒體為網路廣告-Google',
    'L1_ENAME_Newspaper' : '第一層媒體為報紙廣告',
    'L1_ENAME_Message' : '第一層媒體為簡訊廣告',
    'L1_ENAME_B2B Prospect' : '第一層媒體為業務異業開發',
    'L1_ENAME_TV' : '第一層媒體為電視',
    'L1_ENAME_Retail Store' : '第一層媒體為實體店面',
    'L1_ENAME_Special' : '第一層媒體為專案活動',
    'L1_ENAME_From_TutorABC' : '第一層媒體為TutorABC 轉入',
    'L1_ENAME_Campus retailer' : '第一層媒體為校園商店',
    'L1_ENAME_Strategic Partnership' : '第一層媒體為品牌策略合作',
    'L1_ENAME_E-Commerce' : '第一層媒體為電子商城',
    'L1_ENAME_Social Media' : '第一層媒體為社群媒體',
}

for i in range(len(X)):
    shap_values = explainer.shap_values(np.array(X.iloc[i]).reshape((1,-1)))
    shap_array = shap_values[y_pred[i]][0]
    shap_pd = pd.DataFrame(shap_array, index = X.columns).loc[list(mapping.keys()),:].sort_values([0], ascending=False)[0:3].index
    reason1.append(shap_pd[0])
    reason2.append(shap_pd[1])
    reason3.append(shap_pd[2])
    
logging.info('End Analyzing Data')

logging.info('Start Processing Data')

output_df = X.copy()
output_df['contractsn'] = df.contractsn
output_df['reason1'] = reason1
output_df['reason2'] = reason2
output_df['reason3'] = reason3
output_df = output_df.reset_index(drop=True)
output_df = pd.concat([output_df, prob], axis=1)
output_df['DataDate'] = dat
output_df['ETL_DATE'] = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
output_df2 = output_df[['DataDate', 'contractsn', 'pred_prob1', 'reason1', 'reason2', 'reason3', 'ETL_DATE']]

output_df2["reason1"] = output_df2["reason1"].map(mapping)
output_df2["reason2"] = output_df2["reason2"].map(mapping)
output_df2["reason3"] = output_df2["reason3"].map(mapping)

logging.info('Start Writing Data to DB')
write_db = db_operation.pymssql_write(output_df2)

logging.info('End Writing Data to DB')

logging.info('Start Executing SP to DB')

file2 = 'query2.sql'

with open(file2, 'r') as fd:
    sql2 = fd.read()

sp = db_operation.pymssql_sp_cm2_exec(sql2)

logging.info('End Executing SP to DB')
