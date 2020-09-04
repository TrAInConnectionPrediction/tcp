import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from RtdRay import RtdRay
import pandas as pd


def get_delays(rtd_df):
    rtd_df['ar_cancellations'] = rtd_df['ar_cs'] != 'c'
    rtd_df['ar_cancellation_time_delta'] = (rtd_df['ar_clt'] - rtd_df['ar_pt']) / pd.Timedelta(minutes=1)
    rtd_df['ar_delay'] = (rtd_df['ar_ct'] - rtd_df['ar_pt']) / pd.Timedelta(minutes=1)
    ar_mask = (rtd_df['ar_cs'] != 'c') & (rtd_df['ar_delay'].notnull())
    rtd_df['ar_on_time_3'] = rtd_df.loc[ar_mask, 'ar_delay'] < 4
    rtd_df['ar_on_time_5'] = rtd_df.loc[ar_mask, 'ar_delay'] < 6
    rtd_df['ar_fern_on_time_5'] = rtd_df.loc[rtd_df['f'] == 'F', 'ar_on_time_5']

    rtd_df['dp_cancellations'] = rtd_df['dp_cs'] != 'c'
    rtd_df['dp_cancellation_time_delta'] = (rtd_df['dp_clt'] - rtd_df['dp_pt']) / pd.Timedelta(minutes=1)
    rtd_df['dp_delay'] = (rtd_df['dp_ct'] - rtd_df['dp_pt']) / pd.Timedelta(minutes=1)
    dp_mask = (rtd_df['dp_cs'] != 'c') & (rtd_df['dp_delay'].notnull())
    rtd_df['dp_on_time_3'] = rtd_df.loc[dp_mask, 'dp_delay'] < 4
    rtd_df['dp_on_time_5'] = rtd_df.loc[dp_mask, 'dp_delay'] < 6
    rtd_df['dp_fern_on_time_5'] = rtd_df.loc[rtd_df['f'] == 'F', 'dp_on_time_5']

    return rtd_df


def load_with_delay(columns: list, **kwargs):
    delay_cols = ['ar_ct', 'ar_pt', 'dp_ct', 'dp_pt', 'ar_cs', 'ar_clt', 'dp_cs', 'dp_clt']
    for col in delay_cols:
        if col not in columns:
            columns.append(col)
    rtd = RtdRay()
    rtd_df = rtd.load_data(columns=columns, **kwargs)
    return get_delays(rtd_df)
