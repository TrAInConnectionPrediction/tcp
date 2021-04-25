import os, sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import sqlalchemy
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib
from helpers.StationPhillip import StationPhillip
import datetime
from data_analysis.delay import load_with_delay, load_long_distance_with_delay


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
                        'ar_happened': ['mean'],
                        'ar_cancellation_time_delta': ['count', 'mean'],
                        'dp_pt': ['count'],
                        'dp_delay': ['count'],
                        'dp_happened': ['mean'],
                        'dp_cancellation_time_delta': ['count', 'mean'],
                    }).compute()
            self.data = self.data.nlargest(50, columns=('ar_pt', 'count'))
            self.data = self.data.sort_index()
            self.data.to_csv(self._CACHE_PATH)

    def plot_count(self, logy=False):
        fig, ax1 = plt.subplots()
        ax1.tick_params(axis="both", labelsize=20) 
        index = self.data.index.to_numpy()
        ax1.set_xlim(index.min(), index.max())
        ax1.set_yscale('log')

        ax1.grid(True)

        ax1.set_title('Delay distribution', fontsize=50)
        ax1.set_xlabel('Delay in minutes', fontsize=30)
        ax1.set_ylabel('Count', color="blue", fontsize=30)

        ax1.plot(self.data[('ar_pt', 'count')] + self.data[('dp_pt', 'count')],
                    color="blue",
                    linewidth=3,
                    label='Stops')        
        
        fig.legend(fontsize=20)
        ax1.set_ylim(bottom=0)
        plt.show()


if __name__ == '__main__':
    rtd_df = load_with_delay(columns=['station', 'c', 'f'])
    delay = DelayAnalysis(rtd_df, use_cache=True)
    delay.plot_count()

