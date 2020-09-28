import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
from mpl_toolkits.basemap import Basemap
from helpers.StationPhillip import StationPhillip
from delay import load_with_delay


class PerStationAnalysis(StationPhillip):
    FERN_ON_TIME_PLOT = {
        'count_1': 'ar_fern_on_time_5',
        'count_2': 'dp_fern_on_time_5',
        'color_value': 'dp_fern_on_time_5'
    }

    ALL_ON_TIME_PLOT = {
        'count_1': 'ar_delay',
        'count_2': 'dp_delay',
        'color_value': 'dp_on_time_5'
    }

    ALL_CANCELLATIONS_PLOT = {
        'count_1': 'ar_delay',
        'count_2': 'dp_delay',
        'color_value': 'dp_cancellations'
    }

    CACHE_PATH = 'data/per_station_data.csv'

    def __init__(self, rtd_df, use_cache=True):
        super().__init__()
        try:
            if not use_cache:
                raise FileNotFoundError
            self.data = pd.read_csv(self.CACHE_PATH, header=[0, 1], index_col=0)
            print('using cached data')
        except FileNotFoundError:
            self.data = rtd_df.groupby('station', sort=False).agg({
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
            # remove station with less than 500 stops
            self.data = self.data.loc[self.data[('dp_delay', 'count')] > 500, :]

            self.data.to_csv(self.CACHE_PATH)

    def plot(self, data_to_plot):
        left = 5.67
        right = 15.64
        bot = 47.06
        top = 55.06
        plt.figure(figsize=(90, 50))
        m = Basemap(llcrnrlon=left,llcrnrlat=bot,urcrnrlon=right,urcrnrlat=top,
                    resolution='i', projection='tmerc', lat_0 = 51, lon_0 = 10)
        m.drawcoastlines(linewidth=0.72, color='black')
        m.drawcountries(zorder=0, color='black')

        x = np.zeros(len(self.data.index))
        y = np.zeros(len(self.data.index))
        s = np.zeros(len(self.data.index))
        c = np.zeros(len(self.data.index))

        for i, station in enumerate(self.data.index):
            x[i], y[i] = self.get_location(name=station)
            s[i] = (self.data.loc[station, [(data_to_plot['count_1'], 'count')]][0] +
                self.data.loc[station, [(data_to_plot['count_2'], 'count')]][0])
            c[i] = self.data.loc[station, [(data_to_plot['color_value'], 'mean')]]

        s = s / 100
        c = (c - min(c)) / max(c - min(c))

        cmap = matplotlib.colors.LinearSegmentedColormap.from_list("", ["red", 'yellow',"green"])
        m.scatter(x, y, c=c, cmap=cmap, s=s, alpha=0.2, latlon=True)
        plt.show()


if __name__ == '__main__':
    rtd_df = load_with_delay(columns=['station', 'c', 'f'])
    per_station = PerStationAnalysis(rtd_df)
    per_station.plot(per_station.ALL_ON_TIME_PLOT)
