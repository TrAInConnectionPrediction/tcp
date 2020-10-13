import sqlalchemy
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib
from helpers.StationPhillip import StationPhillip
import datetime
from data_analisys.delay import load_with_delay, load_long_distance_with_delay


class DelayAnalysis:
    _CACHE_PATH = 'data/delay_analysis_data.csv'

    def __init__(self, rtd_df, use_cache=True):
        try:
            if not use_cache:
                raise FileNotFoundError
            self.data = pd.read_csv(self._CACHE_PATH, header=[0, 1], index_col=0, parse_dates=[0])
            print('using cached data')
        except FileNotFoundError:
            self.data = rtd_df.groupby('ar_delay').agg({
                        'ar_pt': ['count'],
                        'ar_delay': ['count'],
                        'ar_cancellations': ['mean'],
                        'ar_cancellation_time_delta': ['count', 'mean'],
                        'dp_pt': ['count'],
                        'dp_delay': ['count'],
                        'dp_cancellations': ['mean'],
                        'dp_cancellation_time_delta': ['count', 'mean'],
                    }).compute()
            self.data = self.data.nlargest(50, columns=('ar_pt', 'count'))
            self.data = self.data.sort_index()
            self.data.to_csv(self._CACHE_PATH)

    def plot_count(self):
        self.data.loc[:, ('ar_pt', 'count')].plot(logy=True)
        plt.show()


if __name__ == '__main__':
    rtd_df = load_with_delay(columns=['station', 'c', 'f'])
    # rtd_df = load_long_distance_with_delay(columns=['station', 'c', 'f'])
    delay = DelayAnalysis(rtd_df, use_cache=False)
    delay.plot_count()

