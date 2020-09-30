import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
from helpers.RtdRay import RtdRay
import matplotlib.pyplot as plt


def get_delays(rtd_df):
    rtd_df['ar_cancellations'] = rtd_df['ar_cs'] == 'c'
    rtd_df['ar_cancellation_time_delta'] = (rtd_df['ar_clt'] - rtd_df['ar_pt']) / pd.Timedelta(minutes=1)
    rtd_df['ar_delay'] = (rtd_df['ar_ct'] - rtd_df['ar_pt']) / pd.Timedelta(minutes=1)
    ar_mask = ((rtd_df['ar_cs'] != 'c')) & (rtd_df['ar_delay'].notnull())
    rtd_df['ar_on_time_3'] = rtd_df.loc[ar_mask, 'ar_delay'] < 4
    rtd_df['ar_on_time_5'] = rtd_df.loc[ar_mask, 'ar_delay'] < 6
    rtd_df['ar_fern_on_time_5'] = rtd_df.loc[rtd_df['f'] == 'F', 'ar_on_time_5']

    rtd_df['dp_cancellations'] = rtd_df['dp_cs'] == 'c'
    rtd_df['dp_cancellation_time_delta'] = (rtd_df['dp_clt'] - rtd_df['dp_pt']) / pd.Timedelta(minutes=1)
    rtd_df['dp_delay'] = (rtd_df['dp_ct'] - rtd_df['dp_pt']) / pd.Timedelta(minutes=1)
    dp_mask = ((rtd_df['dp_cs'] != 'c')) & (rtd_df['dp_delay'].notnull())
    rtd_df['dp_on_time_3'] = rtd_df.loc[dp_mask, 'dp_delay'] < 4
    rtd_df['dp_on_time_5'] = rtd_df.loc[dp_mask, 'dp_delay'] < 6
    rtd_df['dp_fern_on_time_5'] = rtd_df.loc[rtd_df['f'] == 'F', 'dp_on_time_5']

    return rtd_df

rtd = RtdRay(notebook=True)

rtd_df = rtd.load_data(columns=['station', 'c', 'ar_ct', 'ar_pt', 'dp_ct', 'dp_pt', 'ar_cs', 'ar_clt', 'dp_cs', 'dp_clt', 'f'])
# rtd_df = rtd_df.loc[rtd_df['station'] == 'Tübingen Hbf', :].compute()
rtd_df = get_delays(rtd_df)
rtd_df = rtd_df.loc[rtd_df['ar_cancellations'] == False, :]
rtd_df = rtd_df.loc[rtd_df['ar_delay'] >= 0, :]
rtd_df = rtd_df.loc[:, ['ar_ct', 'ar_delay']].dropna()
rtd_df = rtd_df.rename(columns={"ar_ct": "ds", "ar_delay": "y"})
rtd_df['ds'] = rtd_df['ds'].dt.round(freq='300min')
rtd_df = rtd_df.groupby('ds').agg({'y': ['mean']}).compute()
rtd_df = rtd_df.loc[rtd_df[('y', 'mean')] < 10, :]
rtd_df[['y']].plot(figsize=(50, 50)) # x='ds', y='y',
plt.show()
print('data part finished')

# m = Prophet()
# m.fit(rtd_df)
#
# future = m.make_future_dataframe(periods=14)
# print(future.tail())
#
# forecast = m.predict(future)
# print(forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail())
#
# fig1 = m.plot(forecast)
