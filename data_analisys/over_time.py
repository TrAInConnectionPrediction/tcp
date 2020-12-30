import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import sqlalchemy
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib
import datetime
import dask.dataframe as dd
from data_analisys.delay import load_with_delay


class OverHour:
    CACHE_PATH = 'data_buffer/hour_analysis_data.csv'

    def __init__(self, rtd_df, use_cache=True):
        try:
            if not use_cache:
                raise FileNotFoundError
            self.data = pd.read_csv(self.CACHE_PATH, header=[0, 1], index_col=0, parse_dates=[0])
            print('using cached data')
        except FileNotFoundError:
            # Use dask Client to do groupby as the groupby is complex and scales well on local cluster.
            from dask.distributed import Client
            client = Client()
            rtd_df['minute'] = rtd_df['ar_pt'].dt.minute
            rtd_df['minute'] = rtd_df['minute'].fillna(value=rtd_df['dp_pt'].dt.minute).astype(int)
            rtd_df['minute'] = rtd_df.map_partitions(self.minutetime, meta=rtd_df['ar_pt'])
            rtd_df = rtd_df.loc[~rtd_df['minute'].isna(), :]
            self.data = rtd_df.groupby('minute').agg({
                        'ar_pt': ['count'],
                        'ar_delay': ['count', 'mean'],
                        'ar_on_time_5': ['mean'],
                        'ar_cancellations': ['mean'],
                        'dp_pt': ['count'],
                        'dp_delay': ['count', 'mean'],
                        'dp_on_time_5': ['mean'],
                        'dp_cancellations': ['mean'],
                    }).compute()
            self.data = self.data.loc[~self.data.index.isna(), :]
            self.data = self.data.sort_index()
            self.data.to_csv(self.CACHE_PATH)

    @staticmethod
    def minutetime(df):
        """
        Create datetime from minutes.

        Parameters
        ----------
        df : pandas.DataFrame
            DataFrame with the columns minutes (0 - 59).

        Returns
        -------
        pd.Series
            daytime and weekday combined on basedate (03-01-2000).
        """
        return pd.Series([datetime.datetime(year=2000, month=1, day=3, minute=minute) for minute in df['minute']], index=df['minute'].index)


    def plot_over_hour(self):
        self.data = self.data
        dt_index = self.data.index.to_numpy()
        minutes_10 = mdates.MinuteLocator(byminute=range(0, 60, 10))

        fig, ax = plt.subplots()
        date_form = mdates.DateFormatter("%M")
        ax.xaxis.set_major_formatter(date_form)
        ax.xaxis.set_major_locator(minutes_10)
        ax.set_xlim(dt_index.min(), dt_index.max())

        ax.set_title('Delay within one hour', fontsize=40)
        ax.set_xlabel("Minute", fontsize=30)
        # ax.plot(dt_index, self.data[('ar_pt', 'count')] + self.data[('dp_pt', 'count')], color="blue")
        # ax.set_ylabel("Stops", color="blue", fontsize=30)
        # ax.grid(True)

        ax2 = ax.twinx()
        ax2.xaxis.set_major_formatter(date_form)
        ax2.xaxis.set_major_locator(minutes_10)
        ax2.plot(dt_index, self.data[('ar_delay', 'mean')], color="red")
        ax2.plot(dt_index, self.data[('dp_delay', 'mean')], color="orange")
        ax2.set_ylabel("Delay", color="orange", fontsize=30)
        ax2.grid(True)

        fig.autofmt_xdate()
        plt.show()


class TimeAnalysis:
    CACHE_PATH = 'data/time_analysis_data.csv'

    def __init__(self, rtd_df, use_cache=True):
        try:
            if not use_cache:
                raise FileNotFoundError
            self.data = pd.read_csv(self.CACHE_PATH, header=[0, 1], index_col=0, parse_dates=[0])
            print('using cached data')
        except FileNotFoundError:
            # Use dask Client to do groupby as the groupby is complex and scales well on local cluster.
            from dask.distributed import Client
            client = Client()
            rtd_df['day'] = rtd_df['ar_pt'].dt.date
            rtd_df['daytime'] = rtd_df['ar_pt'].dt.time
            rtd_df = rtd_df.loc[~rtd_df['daytime'].isna(), :]
            self.data = rtd_df.groupby('daytime').agg({
                        'ar_pt': ['count'],
                        'ar_delay': ['count', 'mean'],
                        'ar_on_time_5': ['mean'],
                        'ar_cancellations': ['mean'],
                        'dp_pt': ['count'],
                        'dp_delay': ['count', 'mean'],
                        'dp_on_time_5': ['mean'],
                        'dp_cancellations': ['mean'],
                    }).compute()
            self.data = self.data.loc[~self.data.index.isna(), :]
            self.data = self.data.sort_index()
            self.data.to_csv(self.CACHE_PATH)

    def plot_over_day(self):
        self.data = self.data
        dt_index = self.data.index.to_numpy()
        hours = mdates.HourLocator()

        fig, ax = plt.subplots()
        date_form = mdates.DateFormatter("%H:%M")
        ax.xaxis.set_major_formatter(date_form)
        ax.xaxis.set_major_locator(hours)
        ax.set_xlim(dt_index.min(), dt_index.max())

        ax.plot(dt_index, self.data[('ar_pt', 'count')] + self.data[('dp_pt', 'count')], color="blue")
        ax.set_xlabel("Daytime", fontsize=14)
        ax.set_ylabel("Stops", color="blue", fontsize=14)

        ax.grid(True)

        ax2 = ax.twinx()
        ax2.xaxis.set_major_formatter(date_form)
        ax2.xaxis.set_major_locator(hours)
        ax2.plot(dt_index, self.data[('ar_delay', 'mean')], color="red")
        ax2.plot(dt_index, self.data[('dp_delay', 'mean')], color="orange")
        ax2.set_ylabel("Delay", color="orange", fontsize=14)
        ax2.grid(True)

        fig.autofmt_xdate()
        plt.show()


class OverWeek:
    CACHE_PATH = 'data/over_week.csv'

    def __init__(self, rtd_df, use_cache=True):
        try:
            if not use_cache:
                raise FileNotFoundError
            self.data = pd.read_csv(self.CACHE_PATH, header=[0, 1], index_col=0, parse_dates=[0])
            print('using cached data')
        except FileNotFoundError:
            # Use dask Client to do groupby as the groupby is complex and scales well on local cluster.
            from dask.distributed import Client
            client = Client()
            rtd_df['day'] = rtd_df['ar_pt'].dt.date
            rtd_df['weekday'] = rtd_df['ar_pt'].dt.dayofweek
            rtd_df = rtd_df.loc[~rtd_df['weekday'].isna(), :]
            rtd_df['daytime'] = rtd_df['ar_pt'].dt.time
            rtd_df['weektime'] = rtd_df.map_partitions(self.weektime, meta=rtd_df['daytime'])
            self.data = rtd_df.groupby(['weektime']).agg({
                        'ar_pt': ['count', 'max'],
                        'ar_delay': ['count', 'mean'],
                        'ar_on_time_5': ['mean'],
                        'ar_cancellations': ['mean'],
                        'dp_pt': ['count'],
                        'dp_delay': ['count', 'mean'],
                        'dp_on_time_5': ['mean'],
                        'dp_cancellations': ['mean'],
                    }).compute()
            self.data = self.data.loc[~self.data.index.isna(), :]
            self.data = self.data.sort_index()
            self.data.to_csv(self.CACHE_PATH)

    @staticmethod
    def weektime(df):
        """
        Create datetime from time and weekday.

        Parameters
        ----------
        df : pandas.DataFrame
            DataFrame with the columns daytime (datetime.time) and weekday (0 - 6).

        Returns
        -------
        pd.Series
            daytime and weekday combined on basedate (03-01-2000).
        """
        return pd.Series([datetime.datetime.combine(datetime.date(year=2000, month=1, day=3), time) for time in df['daytime']], index=df['daytime'].index) \
               + pd.Series(data=[pd.Timedelta(days=day) for day in df['weekday']], index=df['daytime'].index)

    def plot_over_week(self):
        dt_index = self.data.index.to_numpy()
        hours = mdates.HourLocator(interval=8)

        fig, ax = plt.subplots()
        date_form = mdates.DateFormatter("%A %H:%M") # E.g.: Monday 08:00
        ax.xaxis.set_major_formatter(date_form)
        ax.xaxis.set_major_locator(hours)
        ax.set_xlim(dt_index.min(), dt_index.max())

        # Plot count of Stops
        ax.plot(dt_index, self.data[('ar_pt', 'count')] + self.data[('dp_pt', 'count')], color="blue")
        ax.set_xlabel("Daytime", fontsize=14)
        ax.set_ylabel("Stops", color="blue", fontsize=14)
        ax.grid(True)

        # Plot avg delay
        ax2 = ax.twinx()
        ax2.xaxis.set_major_formatter(date_form)
        ax2.xaxis.set_major_locator(hours)
        ax2.plot(dt_index, self.data[('ar_delay', 'mean')], color="red")
        ax2.plot(dt_index, self.data[('dp_delay', 'mean')], color="orange")
        ax2.set_ylabel("Delay", color="orange", fontsize=14)
        ax2.grid(True)

        fig.autofmt_xdate()
        plt.show()


class OverYear:
    CACHE_PATH = 'data/over_year.csv'

    def __init__(self, rtd_df, use_cache=True):
        try:
            if not use_cache:
                raise FileNotFoundError
            self.data = pd.read_csv(self.CACHE_PATH, header=[0, 1], index_col=0, parse_dates=[0])
            print('using cached data')
        except FileNotFoundError:
            # Use dask Client to do groupby as the groupby is complex and scales well on local cluster.
            from dask.distributed import Client
            client = Client()
            rtd_df['floating_hour'] = rtd_df['ar_pt'].dt.hour // 24 * 24
            rtd_df = rtd_df.loc[~rtd_df['floating_hour'].isna(), :]
            rtd_df['date'] = rtd_df['ar_pt'].dt.date
            rtd_df['floating_yeartime'] = rtd_df.map_partitions(self.floating_yeartime, meta=rtd_df['ar_pt'])
            self.data = rtd_df.groupby(['floating_yeartime']).agg({
                        'ar_pt': ['count'],
                        'ar_delay': ['count', 'mean'],
                        'ar_on_time_5': ['mean'],
                        'ar_cancellations': ['mean'],
                        'dp_pt': ['count'],
                        'dp_delay': ['count', 'mean'],
                        'dp_on_time_5': ['mean'],
                        'dp_cancellations': ['mean'],
                    }).compute()
            self.data = self.data.loc[~self.data.index.isna(), :]
            self.data = self.data.sort_index()
            self.data.to_csv(self.CACHE_PATH)

    @staticmethod
    def floating_yeartime(df):
        """
        Create datetime from hour and date.

        Parameters
        ----------
        df : pandas.DataFrame
            DataFrame with the columns date (datetime.date) and hour (0 - 23).

        Returns
        -------
        pd.Series
            date and hour combined.
        """
        return pd.Series([datetime.datetime.combine(row['date'], datetime.time(hour=int(row['floating_hour']))) for i, row in df.iterrows()], index=df['date'].index)

    def plot_over_year(self):
        dt_index = self.data.index.to_numpy()
        hours = mdates.HourLocator(interval=24*2)

        fig, ax = plt.subplots()
        date_form = mdates.DateFormatter("%D")
        ax.xaxis.set_major_formatter(date_form)
        ax.xaxis.set_major_locator(hours)
        ax.set_xlim(dt_index.min(), dt_index.max())

        # Plot count of Stops
        ax.plot(dt_index, self.data[('ar_pt', 'count')] + self.data[('dp_pt', 'count')], color="blue")
        ax.set_xlabel("Daytime", fontsize=14)
        ax.set_ylabel("Stops", color="blue", fontsize=14)
        ax.grid(True)

        # Plot avg delay
        ax2 = ax.twinx()
        ax2.xaxis.set_major_formatter(date_form)
        ax2.xaxis.set_major_locator(hours)
        ax2.plot(dt_index, self.data[('ar_delay', 'mean')], color="red")
        ax2.plot(dt_index, self.data[('dp_delay', 'mean')], color="orange")
        ax2.set_ylabel("Delay", color="orange", fontsize=14)
        ax2.grid(True)

        fig.autofmt_xdate()
        plt.show()


if __name__ == '__main__':
    import helpers.fancy_print_tcp
    rtd_df = load_with_delay(columns=['station', 'c', 'f'])
    time_analysis = OverHour(rtd_df, use_cache=False)
    time_analysis.plot_over_hour()

    # time = TimeAnalysis(rtd_df, use_cache=True)
    # time.plot_over_day()

    # time = OverWeek(rtd_df, use_cache=False)
    # time.plot_over_week()

    # time = OverYear(rtd_df, use_cache=True)
    # time.plot_over_year()
