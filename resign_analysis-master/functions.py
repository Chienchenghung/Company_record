import pandas as pd
import pickle
from sklearn.preprocessing import LabelEncoder, Normalizer
import numpy as np
from sklearn.compose import ColumnTransformer
from imblearn.over_sampling import SMOTENC
from sklearn.model_selection import train_test_split
from itertools import islice



def transform_data(data, type='predict'):

    # LABEL ENCODER
    lbl = LabelEncoder()

    # GET NORMALIZED COLUMNS
    df =data
    col_to_normalized = df.drop(
        columns=['location', 'brand', 'upgrade', 'd', 'sales_name', 'age', 'gender', 'day_upgrade', 'day_lasting',
                 'day_standard', 'depttw']).columns
    # DATA TRANSFORMING
    df['open_new'] = (df['new_cnt'] > 0).astype('int').astype('category')
    df['open_new'] = lbl.fit_transform(df['open_new'].astype(str))
    df['location'] = np.where(df.location == '台中', 1, 0)
    df['location'] = lbl.fit_transform(df['location'].astype(str))
    df['brand'] = np.where(df.brand == 'TutorABC(TP)', 1, 0)
    df['brand'] = lbl.fit_transform(df['brand'].astype(str))
    df['upgrade'] = lbl.fit_transform(df['upgrade'].astype(str))
    df['dayoff'] = lbl.fit_transform(df['dayoff'].astype(str))
    df['wk'] = df.d // 7
    df['datepart'] = df.d % 7
    df['d'] = lbl.fit_transform(df['d'])

    #NORMALIZATION
    df_to_normalized = df[col_to_normalized]

    # READ MODEL AND TRANSFORMER
    if type == 'training':
        nml_preprocessor = ColumnTransformer(remainder='passthrough',
                                             transformers=[('normalize', Normalizer(), col_to_normalized)])
        nml_preprocessor.fit(df_to_normalized)
        pickle.dump(nml_preprocessor, open("./pickle/nml_new.pkl", "wb"))


    else:
        nml_preprocessor = pickle.load(open("./pickle/nml.pkl", "rb"))

    df_rest = df.drop(columns=col_to_normalized)
    df_normalized = pd.concat([df_rest, pd.DataFrame(10 * nml_preprocessor.transform(df_to_normalized),
                                                     index=df_to_normalized.index, columns=df_to_normalized.columns)],
                              axis=1)
    del (df_to_normalized)
    del (df_rest)

    df_normalized[
        ['stv_t', 'dealcnt_t', 'new_cnt_t', 'closed_cnt_t', 'reserv_new_t', 'reserv_closed_t', 'dayoff_t', 'work_hr_t',
         'scrm_new_cnt_t', 'reserv_scrm_new_t']] = df_normalized.groupby(['sales_name', 'datepart'])[
        ['stv', 'dealcnt', 'new_cnt', 'closed_cnt', 'reserv_new', 'reserv_closed', 'dayoff', 'work_hr', 'scrm_new_cnt',
         'reserv_scrm_new']].diff().fillna(0)
    df_normalized[
        ['stv_ma3_t', 'dealcnt_ma3_t', 'new_cnt_ma3_t', 'closed_cnt_ma3_t', 'reserv_new_ma3_t', 'reserv_closed_ma3_t',
         'dayoff_ma3_t', 'work_hr_ma3_t', 'scrm_new_cnt_ma3_t', 'reserv_scrm_new_ma3_t']] = \
    df_normalized.groupby(['sales_name', 'datepart'])[
        ['stv_ma3', 'dealcnt_ma3', 'new_cnt_ma3', 'closed_cnt_ma3', 'reserv_new_ma3', 'reserv_closed_ma3', 'dayoff_ma3',
         'work_hr_ma3', 'scrm_new_cnt_ma3', 'reserv_scrm_new_ma3']].diff().fillna(0)
    df_normalized[
        ['stv_ma7_t', 'dealcnt_ma7_t', 'new_cnt_ma7_t', 'closed_cnt_ma7_t', 'reserv_new_ma7_t', 'reserv_closed_ma7_t',
         'dayoff_ma7_t', 'work_hr_ma7_t', 'scrm_new_cnt_ma7_t', 'reserv_scrm_new_ma7_t']] = \
    df_normalized.groupby(['sales_name', 'datepart'])[
        ['stv_ma7', 'dealcnt_ma7', 'new_cnt_ma7', 'closed_cnt_ma7', 'reserv_new_ma7', 'reserv_closed_ma7', 'dayoff_ma7',
         'work_hr_ma7', 'scrm_new_cnt_ma7', 'reserv_scrm_new_ma7']].diff().fillna(0)
    df_normalized[['stv_ma14_t', 'dealcnt_ma14_t', 'new_cnt_ma14_t', 'closed_cnt_ma14_t', 'reserv_new_ma14_t',
                   'reserv_closed_ma14_t', 'dayoff_ma14_t', 'work_hr_ma14_t', 'scrm_new_cnt_ma14_t',
                   'reserv_scrm_new_ma14_t']] = df_normalized.groupby(['sales_name', 'datepart'])[
        ['stv_ma14', 'dealcnt_ma14', 'new_cnt_ma14', 'closed_cnt_ma14', 'reserv_new_ma14', 'reserv_closed_ma14',
         'dayoff_ma14', 'work_hr_ma14', 'scrm_new_cnt_ma14', 'reserv_scrm_new_ma14']].diff().fillna(0)

    return df_normalized


def data_for_model(data):
    df_normalized = data
    df_normalized['leave_in_60'] = np.where(df_normalized.d >= df_normalized.day_lasting - 60, 1, 0)

    df_normalized = df_normalized[
        ~((pd.isna(df_normalized.day_lasting)) & (df_normalized.d > df_normalized.day_standard - 60))]
    df_training = df_normalized[(~pd.isna(df_normalized.day_lasting)) | (~pd.isna(df_normalized.day_upgrade))]

    #
    df_training_sales = pd.unique(df_training.sales_name)
    df_training_sales_train, x_60_sales_test = train_test_split(df_training_sales, test_size=0.2, random_state=123)

    x_train_60 = df_training[df_training.sales_name.isin(df_training_sales_train)].drop(
        columns=['sales_name', 'leave_in_60', 'day_lasting', 'day_upgrade', 'wk', 'datepart', 'day_standard', 'depttw'])
    y_train_60 = df_training[df_training.sales_name.isin(df_training_sales_train)]['leave_in_60']
    x_test_60 = df_training[~df_training.sales_name.isin(df_training_sales_train)].drop(
        columns=['sales_name', 'leave_in_60', 'day_lasting', 'day_upgrade', 'wk', 'datepart', 'day_standard', 'depttw'])
    y_test_60 = df_training[~df_training.sales_name.isin(df_training_sales_train)]['leave_in_60']

    #
    catcol = list(map(lambda col: x_train_60.columns.get_loc(col),
                      ['d', 'dayoff', 'upgrade', 'brand', 'gender', 'location', 'open_new']))
    sm = SMOTENC(random_state=27, categorical_features=catcol)
    x_train_60, y_train_60 = sm.fit_resample(x_train_60, y_train_60)

    return x_train_60, y_train_60, x_test_60, y_test_60


def take(n, iterable):
    return list(islice(iterable, n))


def topN(row, n):
    x = row.to_dict() # convert the input row to a dictionary
    x = {k: v for k, v in sorted(x.items(), key=lambda item: -item[1])} # sort the dictionary based on their values
    n_items = take(n, x.items()) # extract the first n values from the dictionary
    return n_items

def botN(row, n):
    x = row.to_dict() # convert the input row to a dictionary
    x = {k: v for k, v in sorted(x.items(), key=lambda item: -item[1], reverse=True)} # sort the dictionary based on their values
    n_items = take(n, x.items()) # extract the first n values from the dictionary
    return n_items

