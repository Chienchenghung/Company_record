import pandas as pd
from metabase_api import Metabase_API
import xgboost as xgb
from sklearn.model_selection import RandomizedSearchCV
import numpy as np
from sklearn.metrics import f1_score
import pickle
import time
from functions import transform_data, data_for_model
import csv
#
print('Extracting data from Metabase')
mb = Metabase_API("http://metabase.tutorabc.com", 'mingkao@tutorabc.com', '@F12843')
res = mb.post("/api/card/{}/query/{}".format(3024, 'json'))
df = pd.json_normalize(res).drop(columns=['is_upgrade', 'hiredate', 'leavedate', 'upgrade_date'])

print('Transforming data')
df_normalized = transform_data(df, type='training')
print('Done with data transforming')

print('Preparing data for modeling')
x_train_60, y_train_60, x_test_60, y_test_60 = data_for_model(df_normalized)
print('Done with data preparation')

# MODEL
print('Starting modeling')
clf = xgb.XGBClassifier(objective='binary:logistic', use_label_encoder=False, random_state=42)
param_grid = {
        'silent': [False],
        'max_depth': [int(x) for x in np.linspace(3, 20, num=11)],
        'learning_rate': [0.001, 0.01, 0.1, 0.2, 0,3],
        'subsample': [0.5, 0.6, 0.7, 0.8, 0.9],
        'colsample_bytree': [0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
        'colsample_bylevel': [0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
        'min_child_weight': [0.5, 1.0, 3.0, 5.0, 7.0, 10.0],
        'gamma': [0, 0.25, 0.5, 1.0],
        'reg_lambda': [0.1, 1.0, 5.0, 10.0, 50.0, 100.0],
#       'num_parallel_tree': [int(x) for x in np.linspace(3, 20, num=11)],
        'n_estimators': [int(x) for x in np.linspace(start=50, stop=300, num=125)]}

fit_params = {'eval_metric': 'logloss',
              'early_stopping_rounds': 10}

rs_clf = RandomizedSearchCV(clf, param_grid, n_iter=20,
                            n_jobs=-1, verbose=2, cv=5,
#                           fit_params=fit_params,
                            scoring='f1_weighted', random_state=42)
# FIRST TRY
print("Randomized search..")
search_time_start = time.time()
rs_clf.fit(x_train_60, y_train_60)
print("Randomized search time:", time.time() - search_time_start)

# SECOND TRY
print('Eliminated low importance features')
thresh = 0.01
cond = np.where(pd.Series(rs_clf.best_estimator_.feature_importances_ > thresh, index=x_train_60.columns)==True)
fs_feature = pd.DataFrame(rs_clf.best_estimator_.feature_importances_, index=x_train_60.columns).iloc[cond].index
select_X_train = x_train_60[fs_feature]
select_X_test = x_test_60[fs_feature]
rs_clf_fs = RandomizedSearchCV(clf, param_grid, n_iter=20,
                            n_jobs=-1, verbose=2, cv=5,
#                           fit_params=fit_params,
                            scoring='f1_weighted', random_state=42)
search_time_start = time.time()
rs_clf_fs.fit(select_X_train, y_train_60)
print("Randomized search time:", time.time() - search_time_start)

# CURRENT MODEL
print('Loading current model')
model = pickle.load(open("./pickle/rf_cls_fs.pkl","rb"))
with open('./result/xgb_features.csv') as csv_file:
    csv_reader = csv.reader(csv_file)
    list_of_columns = []
    for row in csv_reader:
        list_of_columns.extend(row)

select_X_test_current = x_test_60[list_of_columns]

# MODEL COMPARISON
f1_score_new = f1_score(y_test_60,rs_clf_fs.predict(select_X_test))
f1_score_current = f1_score(y_test_60,model.predict(select_X_test_current))

if f1_score_new > f1_score_current:
    print('Got a better model')
    pickle.dump(rs_clf_fs, open("./pickle/rf_cls_fs.pkl", "wb"))
    pickle.dump(fs_feature, open("./pickle/xgb_features.pkl", "wb"))
    pickle.dump(pickle.load(open("./pickle/nml_new.pkl", "rb")), open("./pickle/nml.pkl", "wb"))
    print('Updated Model')
else:
    print('Did not do anything')
