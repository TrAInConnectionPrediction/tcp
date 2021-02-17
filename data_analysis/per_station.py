import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import numpy as np
import datetime
import matplotlib.pyplot as plt
import matplotlib
from mpl_toolkits.basemap import Basemap
# Install Basemap on Linux: https://stackoverflow.com/questions/46560591/how-can-i-install-basemap-in-python-3-matplotlib-2-on-ubuntu-16-04
from helpers.StationPhillip import StationPhillip
from helpers.RtdRay import RtdRay
import matplotlib as mpl
from data_analysis.delay import load_with_delay


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


class PerStationOverTime(StationPhillip):
    PER_STATION_OVER_TIME = 'cache/per_station_over_time.csv'
    FREQ = '1H'

    def __init__(self, rtd, use_cache=True):
        super().__init__()

        try:
            if not use_cache:
                raise FileNotFoundError
            self.data = pd.read_csv(self.PER_STATION_OVER_TIME,
                                    header=[0, 1],
                                    index_col=0,
                                    parse_dates=[[('stop_hour', 'first')]]).reset_index().drop(columns=[0], axis=0)
            self.data.rename(columns = {"('stop_hour', 'first')": ('stop_hour', 'first'),
                                        ('station', 'first'): 'station'}, inplace = True)
            print('using cached data')
        except FileNotFoundError:
            # Use dask Client to do groupby as the groupby is complex and scales well on local cluster.
            from dask.distributed import Client
            client = Client()

            rtd['stop_time'] = rtd['ar_pt'].fillna(value=rtd['dp_pt'])
            rtd = rtd.loc[rtd['stop_time'] > datetime.datetime(2021, 2, 1)].persist() # .compute()
            rtd['stop_hour'] = rtd['stop_time'].dt.round(self.FREQ)
            rtd['str_stop_hour'] = rtd['stop_hour'].astype('str') # rtd['stop_hour'].dt.strftime('%Y-%m-%dT%H:%M:%S.%f%z')

            rtd['single_index_for_groupby'] = rtd['str_stop_hour'] + rtd['station'].astype('str')

            rtd = rtd.set_index('stop_time')

            def per_group(rtd):
                return rtd.resample('1H').agg({
                        'ar_delay': ['count', 'mean'],
                        'ar_cancellations': ['sum', 'mean'],
                        'dp_delay': ['count', 'mean'],
                        'dp_cancellations': ['sum', 'mean'],
                    })

            def resample_rolling(rtd):
                return rtd.resample('1H').agg({
                        'ar_delay': ['count', 'mean'],
                        'ar_cancellations': ['sum', 'mean'],
                        'dp_delay': ['count', 'mean'],
                        'dp_cancellations': ['sum', 'mean'],
                        'stop_hour': ['first'],
                        'station': ['first'],
                    })

            # self.data = rtd_df.groupby('station').apply(per_group).compute()

            self.data = rtd.groupby('single_index_for_groupby', sort=False).agg({
                        'ar_delay': ['count', 'mean'],
                        'ar_cancellations': ['sum', 'mean'],
                        'dp_delay': ['count', 'mean'],
                        'dp_cancellations': ['sum', 'mean'],
                        'stop_hour': ['first'],
                        'station': ['first'],
                    }).compute()
            # remove station with less than 5 stops
            # self.data = self.data.loc[(self.data[('ar_delay', 'count')] + self.data[('dp_delay', 'count')]) >= 5]

            self.data.to_csv(self.PER_STATION_OVER_TIME)

    def animate(self):
        self.data = self.data.loc[self.data[('stop_hour', 'first')] > datetime.datetime(2021, 2, 1, hour=0)]
        for date in pd.date_range(start=self.data[('stop_hour', 'first')].min(), end=self.data[('stop_hour', 'first')].max(), freq=self.FREQ):
            # Bounding Box of Germany
            left = 5.67
            right = 15.64
            bot = 47.06
            top = 55.06
            plt.figure(figsize=(9, 5))
            m = Basemap(llcrnrlon=left,llcrnrlat=bot,urcrnrlon=right,urcrnrlat=top,
                        resolution='i', projection='tmerc', lat_0 = 51, lon_0 = 10)
            m.drawcoastlines(linewidth=0.72, color='black')
            m.drawcountries(zorder=0, color='black')
            
            current_data = self.data.loc[self.data[('stop_hour', 'first')] == date]
            if not current_data.empty:
                current_data = current_data.set_index('station')

                x = np.zeros(len(current_data.index))
                y = np.zeros(len(current_data.index))
                s = np.zeros(len(current_data.index))
                c = np.zeros(len(current_data.index))

                for i, station in enumerate(current_data.index):
                    x[i], y[i] = self.get_location(name=station)

                s[:] = current_data.loc[:, [('ar_cancellations', 'sum')]].to_numpy()[:, 0] \
                    + current_data.loc[:, [('dp_cancellations', 'sum')]].to_numpy()[:, 0]
                c[:] = current_data.loc[:, [('ar_delay', 'mean')]].to_numpy()[:, 0]

                s = s / 2
                # c = (c - min(c)) / max(c - min(c))
                # norm = mpl.colors.Normalize(vmin=0, vmax=7)
                # c[c > 5] = 7
                # c[c < 0] = 0

                cmap = matplotlib.colors.LinearSegmentedColormap.from_list("", ["green", 'yellow',"red"])
                sc = m.scatter(x, y, c=c, cmap=cmap, vmin=0, vmax=7, s=s, alpha=0.2, latlon=True)
                plt.colorbar(sc)

            str_date = date.strftime("%Y-%m-%d %H_%M_%S")
            print(str_date)
            plt.title(str_date)
            plt.savefig(f'data/animation/{str_date}.jpg')
            # plt.show()





if __name__ == '__main__':
    import helpers.fancy_print_tcp
    rtd_ray = RtdRay()
    rtd_df = rtd_ray.load_data(columns=['ar_pt',
                                        'dp_pt',
                                        'station',
                                        'ar_delay',
                                        'ar_cancellations',
                                        'dp_delay',
                                        'dp_cancellations'])
    # per_station = PerStationAnalysis(rtd_df, use_cache=True)
    # per_station.plot(per_station.ALL_ON_TIME_PLOT)

    per_station_time = PerStationOverTime(rtd_df, use_cache=True)
    per_station_time.animate()
