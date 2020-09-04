import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import dask
import dask.dataframe as dd
from dask.diagnostics import ProgressBar

import sqlalchemy
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
from RtdRay import RtdRay
from mpl_toolkits.basemap import Basemap
from helpers.StationPhillip import StationPhillip
from packed_bubbles import BubbleChart
from delay import load_with_delay
from sklearn.preprocessing import normalize


class TrainTypeAnalysis:
    def __init__(self, rtd_df, use_cache=True):
        try:
            if not use_cache:
                raise FileNotFoundError
            self.data = pd.read_csv('data/train_type_data.csv', header=[0, 1], index_col=0)
            print('using cached data')
        except FileNotFoundError:
            self.data = rtd_df.groupby('c', sort=False).agg({
                        'ar_delay': ['count', 'mean'],
                        'ar_on_time_3': ['mean'],
                        'ar_on_time_5': ['mean'],
                        'ar_cancellations': ['mean'],
                        'ar_cancellation_time_delta': ['count', 'mean'],
                        'ar_fern_on_time_5': ['count', 'mean'],
                        'dp_delay': ['count', 'mean'],
                        'dp_on_time_3': ['mean'],
                        'dp_on_time_5': ['mean'],
                        'dp_cancellations': ['mean'],
                        'dp_cancellation_time_delta': ['count', 'mean'],
                        'dp_fern_on_time_5': ['count', 'mean'],
                    }).compute()
            # group train types that are uncommon
            count = self.data[('ar_delay', 'count')] + self.data[('dp_delay', 'count')]
            cutoff = count.nsmallest(50).max()
            combine_mask = count <= cutoff
            groups_to_combine = self.data.loc[combine_mask, :]
            other = self.data.iloc[0, :].copy()
            for col in groups_to_combine.columns:
                if 'count' in col:
                    count_col = col
                    count = groups_to_combine[col].sum()
                    other.loc[col] = count
                else:
                    other.loc[col] = (groups_to_combine[col] * groups_to_combine[count_col]).sum() / count
            self.data = self.data.loc[~combine_mask, :]
            self.data.loc['other', :] = other

            self.data.to_csv('data/train_type_data.csv')

    def plot_type_count(self):
        fig, ax = plt.subplots(subplot_kw=dict(aspect="equal"))
        ax.axis("off")
        bubble_plot = BubbleChart(area=self.data[('ar_delay', 'count')] + self.data[('dp_delay', 'count')], bubble_spacing=40)
        bubble_plot.collapse(n_iterations=2000)
        bubble_plot.plot(ax, labels=self.data.index, colors=['#5A69AF' for _i in range(len(self.data))])
        ax.relim()
        ax.autoscale_view()
        plt.show()

    def plot_type_delay(self, color_by='on_time_3'):
        delays = ((self.data[('ar_' + color_by, 'mean')] + self.data[('dp_' + color_by, 'mean')]) / 2).to_numpy()
        use_trains = np.logical_not(np.isnan(delays))
        delays = delays[use_trains]
        delays = (delays - delays.min()) / max(delays - delays.min())
        fig, ax = plt.subplots(subplot_kw=dict(aspect="equal"))
        ax.axis("off")
        type_count = (self.data.loc[use_trains, ('ar_delay', 'count')] + self.data.loc[use_trains, ('dp_delay', 'count')]).to_numpy()

        cmap = matplotlib.cm.get_cmap('RdYlGn')
        bubble_plot = BubbleChart(area=type_count, bubble_spacing=40)
        bubble_plot.collapse(n_iterations=2000)
        bubble_plot.plot(ax, labels=self.data.loc[use_trains, :].index, colors=[cmap(delay) for delay in delays])
        ax.relim()
        ax.autoscale_view()
        plt.show()


if __name__ == '__main__':
    rtd_df = load_with_delay(columns=['station', 'c', 'f'])
    tta = TrainTypeAnalysis(rtd_df=rtd_df, use_cache=True)
    tta.plot_type_delay(color_by='on_time_5')
