from datetime import datetime
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers.RtdRay import RtdRay
import pickle
import pandas as pd


def get_delays(rtd_df):
    rtd_df['ar_cancellations'] = rtd_df['ar_cs'] != 'c'
    rtd_df['ar_cancellation_time_delta'] = (rtd_df['ar_clt'] - rtd_df['ar_pt']) / pd.Timedelta(minutes=1)
    rtd_df['ar_delay'] = (rtd_df['ar_ct'] - rtd_df['ar_pt']) / pd.Timedelta(minutes=1)
    ar_mask = (rtd_df['ar_cs'] != 'c') & (rtd_df['ar_delay'].notnull())
    rtd_df['ar_on_time_5'] = rtd_df.loc[ar_mask, 'ar_delay'] < 6

    rtd_df['dp_cancellations'] = rtd_df['dp_cs'] != 'c'
    rtd_df['dp_cancellation_time_delta'] = (rtd_df['dp_clt'] - rtd_df['dp_pt']) / pd.Timedelta(minutes=1)
    rtd_df['dp_delay'] = (rtd_df['dp_ct'] - rtd_df['dp_pt']) / pd.Timedelta(minutes=1)
    dp_mask = (rtd_df['dp_cs'] != 'c') & (rtd_df['dp_delay'].notnull())
    rtd_df['dp_on_time_5'] = rtd_df.loc[dp_mask, 'dp_delay'] < 6

    return rtd_df


def load_with_delay(columns: list, **kwargs):
    delay_cols = ['ar_ct', 'ar_pt', 'dp_ct', 'dp_pt', 'ar_cs', 'ar_clt', 'dp_cs', 'dp_clt']
    for col in delay_cols:
        if col not in columns:
            columns.append(col)
    rtd = RtdRay()
    rtd_df = rtd.load_data(columns=columns, **kwargs)
    return get_delays(rtd_df)


def load_long_distance_with_delay(columns: list, **kwargs):
    delay_cols = ['ar_ct', 'ar_pt', 'dp_ct', 'dp_pt', 'ar_cs', 'ar_clt', 'dp_cs', 'dp_clt']
    for col in delay_cols:
        if col not in columns:
            columns.append(col)
    rtd = RtdRay()
    rtd_df = rtd.load_data(columns=columns, filter=[[('f', '=', 'F')]], **kwargs)
    return get_delays(rtd_df)


def load_for_ml_model(max_date, min_date):
    from dask_ml import preprocessing
    rtd_df = load_with_delay(['station', 'f', 'o', 'c', 'n', 'distance_to_start', 'distance_to_end'])
    rtd_df = rtd_df.loc[((rtd_df['ar_pt'] > min_date) | (rtd_df['dp_pt'] > min_date)) &
                        ((rtd_df['ar_pt'] < max_date) | (rtd_df['dp_pt'] < max_date))]
    rtd_df['hour'] = rtd_df['ar_pt'].dt.hour
    rtd_df['hour'] = rtd_df['hour'].fillna(value=rtd_df.loc[:, 'dp_pt'].dt.hour)
    rtd_df['day'] = rtd_df['ar_pt'].dt.dayofweek
    rtd_df['day'] = rtd_df['day'].fillna(value=rtd_df.loc[:, 'dp_pt'].dt.dayofweek)
    # rtd_df['stay_time'] = (rtd_df['dp_pt'] - rtd_df['ar_pt']) / pd.Timedelta(minutes=1)
    for key in ['o', 'c', 'n', 'station']:
        le = preprocessing.LabelEncoder()
        le.fit(rtd_df[key])
        tranformed = le.transform(le.classes_)
        pickle.dump(pd.DataFrame(index=le.classes_, data=tranformed).to_dict()[0], open("data_buffer/LabelEncoder_{}.pkl".format(key), "wb"))
        rtd_df[key] = le.transform(rtd_df[key])
    return rtd_df.drop(columns=['ar_cancellations', 'ar_cancellation_time_delta',
                                'ar_on_time_3', 'ar_on_time_5',
                                'dp_cancellations', 'dp_cancellation_time_delta', 'f',
                                'dp_on_time_3', 'dp_on_time_5', 'ar_ct',
                                'ar_pt', 'dp_ct', 'dp_pt', 'ar_cs', 'ar_clt', 'dp_cs', 'dp_clt'], axis=0)
