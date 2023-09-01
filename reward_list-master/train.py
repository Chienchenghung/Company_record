#!/usr/bin/env python 

from imblearn.under_sampling import RandomUnderSampler
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.metrics import classification_report, auc, accuracy_score, precision_score, recall_score, f1_score
from sklearn.preprocessing import StandardScaler
from lightgbm import LGBMClassifier
from datetime import date, timedelta, datetime

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

td = date.today()
tdt = datetime.now()
dat = datetime.strftime(td,"%Y-%m-%d")
dattime = datetime.strftime(tdt, '%Y-%m-%d %H:%M:%S')

# 輸入資料

logging.info('Read Data From Database')

file = 'query.sql'

with open(file, 'r') as fd:
 	sql = fd.read()

df = db_operation.pymssql_sp_cm2(sql)

df["end_contract_time"] = pd.to_datetime(df["end_contract_time"])

logging.info('Data with size ' + str(df.shape))
       
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
X = X[X.end_contract_time <= dat]
X = X.drop(['end_contract_time'], axis = 1)

y = y[y.end_contract_time <= dat]
y = y.drop(['end_contract_time'], axis = 1)

logging.info('Resampling Data')

undersample = RandomUnderSampler(sampling_strategy='majority')

x_train, x_test, y_train, y_test = train_test_split(X, y.values.ravel(), test_size=0.3)
x_train, y_train = undersample.fit_resample(x_train, y_train)

x_train = pd.DataFrame(x_train, columns=X.columns)
x_test = pd.DataFrame(x_test, columns=X.columns)

logging.info('Training Data with size ' + str(x_train.shape))

logging.info('Testing Data with size ' + str(x_test.shape))

logging.info('Start Training Data')

clf = LGBMClassifier(objective='binary')

param_dist = {
                'n_estimators':range(150,200,10)
                , 'reg_alpha': np.arange(0.8,1,0.05)
                , 'reg_lambda': np.arange(0.8,1,0.05)
                , 'learning_rate':np.arange(0.1,0.3,0.1)
             }

grid = GridSearchCV(clf, param_dist, cv = 5, scoring = 'accuracy', n_jobs = -1, verbose = 1)
grid.fit(x_train, y_train)
y_pred = grid.predict(x_test)

print(classification_report(y_test, y_pred))

logging.info('End Training Data')

joblib.dump(grid.best_estimator_, "award_list_gbm.pkl", compress = 1)

logging.info('End of Training Model')