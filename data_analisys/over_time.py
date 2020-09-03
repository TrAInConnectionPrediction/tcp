import sqlalchemy
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
from helpers.StationPhillip import StationPhillip
import datetime
from delay import load_with_delay


class TimeAnalysis:
    CACHE_PATH = 'data/time_analysis_data.csv'

    def __init__(self, rtd_df, use_cache=True):
        try:
            if not use_cache:
                raise FileNotFoundError
            self.data = pd.read_csv(self.CACHE_PATH, header=[0, 1], index_col=0)
            print('using cached data')
        except FileNotFoundError:
            rtd_df['day'] = rtd_df['ar_pt'].dt.date # round(freq='D')
            rtd_df['daytime'] = rtd_df['ar_pt'].dt.time # - rtd_df['day']
            self.data = rtd_df.groupby('daytime').agg({
                        'ar_pt': ['count'],
                        'ar_delay': ['count', 'mean'],
                        'ar_on_time_3': ['mean'],
                        'ar_on_time_5': ['mean'],
                        'ar_cancellations': ['mean'],
                        'ar_cancellation_time_delta': ['count', 'mean'],
                        'ar_fern_on_time_5': ['count', 'mean'],
                        'dp_pt': ['count'],
                        'dp_delay': ['count', 'mean'],
                        'dp_on_time_3': ['mean'],
                        'dp_on_time_5': ['mean'],
                        'dp_cancellations': ['mean'],
                        'dp_cancellation_time_delta': ['count', 'mean'],
                        'dp_fern_on_time_5': ['count', 'mean'],
                    }).compute()

            self.data.to_csv(self.CACHE_PATH)

    def plot_over_day(self):
        fig, ax = plt.subplots()
        ax.plot(self.data.index, self.data[('ar_pt', 'count')] + self.data[('dp_pt', 'count')], color="blue")
        ax.set_xlabel("Daytime", fontsize=14)
        ax.set_ylabel("Stops", color="blue", fontsize=14)

        ax2 = ax.twinx()
        ax2.plot(self.data.index, self.data[('ar_delay', 'mean')], color="orange")
        ax2.set_ylabel("Delay", color="orange", fontsize=14)

        # self.data[[('ar_delay', 'count'), ('dp_delay', 'count')]].plot()
        plt.show()


if __name__ == '__main__':
    rtd_df = load_with_delay(columns=['station', 'c', 'f'])
    time = TimeAnalysis(rtd_df, use_cache=True)
    time.plot_over_day()

