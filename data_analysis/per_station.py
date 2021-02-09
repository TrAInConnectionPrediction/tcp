import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
from mpl_toolkits.basemap import Basemap
# Install Basemap on Linux: https://stackoverflow.com/questions/46560591/how-can-i-install-basemap-in-python-3-matplotlib-2-on-ubuntu-16-04
from helpers.StationPhillip import StationPhillip
from helpers.RtdRay import RtdRay
from data_analisys.delay import load_with_delay


class PerStationAnalysis(StationPhillip):
    ALL_ON_TIME_PLOT = {
        'count_1': 'ar_delay',
        'count_2': 'dp_delay',
        'color_value': 'dp_delay'
    }

    ALL_CANCELLATIONS_PLOT = {
        'count_1': 'ar_delay',
        'count_2': 'dp_delay',
        'color_value': 'dp_cancellations'
    }

    CACHE_PATH = 'cache/per_station_data.csv'

    def __init__(self, rtd_df, use_cache=True):
        super().__init__()
        try:
            if not use_cache:
                raise FileNotFoundError
            self.data = pd.read_csv(self.CACHE_PATH, header=[0, 1], index_col=0)
            print('using cached data')
        except FileNotFoundError:
            # Use dask Client to do groupby as the groupby is complex and scales well on local cluster.
            from dask.distributed import Client
            client = Client()

            self.data = rtd_df.groupby('station', sort=False).agg({
                        'ar_delay': ['count', 'mean'],
                        'ar_cancellations': ['mean'],
                        'dp_delay': ['count', 'mean'],
                        'dp_cancellations': ['mean'],
                    }).compute()
            # remove station with less than 1000 stops
            self.data = self.data.loc[self.data[('dp_delay', 'count')] > 2000, :]

            self.data.to_csv(self.CACHE_PATH)

    def plot(self, data_to_plot):
        self.data = self.data.loc[self.data[('dp_delay', 'count')] > 2000, :]

        # Bounding Box of Germany
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
    import fancy_print_tcp
    rtd_ray = RtdRay()
    rtd_df = rtd_ray.load_data(columns=['ar_pt',
                                        'dp_pt',
                                        'station',
                                        'ar_delay',
                                        'ar_cancellations',
                                        'dp_delay',
                                        'dp_cancellations'])
    per_station = PerStationAnalysis(rtd_df, use_cache=True)
    per_station.plot(per_station.ALL_ON_TIME_PLOT)
