import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker
import datetime
from helpers.RtdRay import RtdRay


def add_rolling_mean(df: pd.DataFrame, columns: list, window=3) -> pd.DataFrame:
    """
    Add rolling mean to periotic data.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame in which the mean should be added.
    columns : list
        The columns on which the rolling mean should be computed.
    window : int, optional
        The window use for rolling; must be odd, by default 3

    Returns
    -------
    pd.DataFrame
        df with added rolling columns.

    Raises
    ------
    Exception
        Raises if window is not odd.
    """
    if window % 2 == 0:
        raise Exception('window must be odd')

    # Add last columns in the front and first columns in the end
    max_index = len(df) - 1
    dist_from_center = window // 2
    for i in range(dist_from_center):
        df.loc[df.index[0] - df.index[i+1] + df.index[0], :] = df.iloc[max_index - i, :]
        df.loc[df.index[max_index] + (df.index[i+1] - df.index[0]), :] = df.iloc[i, :]
    df = df.sort_index()

    # Calculate rolling mean on columns
    for col in columns:
        if isinstance(col, tuple):
            new_col_name = (col[0], col[1] + '_rolling_mean')
        else:
            new_col_name = col + '_rolling_mean'
        df[new_col_name] = df[col].rolling(window, center=True).mean()
    return df.iloc[dist_from_center:-dist_from_center]


def plot(data, title, x_label, y1_label, y2_label, formatter, locator, ax1_ylim_bottom=None, ax2_ylim_bottom=None, nticks=5):
    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()
    ax1.tick_params(axis="both", labelsize=20) 
    ax2.tick_params(axis="both", labelsize=20) 
    ax1.xaxis.set_major_formatter(formatter)
    ax1.xaxis.set_major_locator(locator)
    ax2.xaxis.set_major_formatter(formatter)
    ax2.xaxis.set_major_locator(locator)
    dt_index = data.index.to_numpy()
    ax1.set_xlim(dt_index.min(), dt_index.max())

    ax1.grid(True)
    ax2.grid(True)

    # Align grids of ax1 and ax2 
    ax1.yaxis.set_major_locator(matplotlib.ticker.LinearLocator(nticks))
    ax2.yaxis.set_major_locator(matplotlib.ticker.LinearLocator(nticks))

    ax1.set_title(title, fontsize=50)
    ax1.set_xlabel(x_label, fontsize=30)
    ax1.set_ylabel(y1_label, color="blue", fontsize=30)
    ax2.set_ylabel(y2_label, color="orange", fontsize=30)

    ax1.plot(data[('ar_delay', 'count_rolling_mean')] + data[('dp_delay', 'count_rolling_mean')],
                color="blue",
                linewidth=3,
                label='Stops')        
    ax2.plot(data[('ar_delay', 'mean_rolling_mean')],
                color="red",
                linewidth=3,
                label='Arrival delay')
    ax2.plot(data[('dp_delay', 'mean_rolling_mean')],
                color="orange",
                linewidth=3,
                label='Departure delay')
    
    fig.legend(fontsize=20)
    ax1.set_ylim(bottom=ax1_ylim_bottom)
    ax2.set_ylim(bottom=ax2_ylim_bottom)
    fig.autofmt_xdate()
    plt.show()


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
                        'ar_delay': ['count', 'mean'],
                        'ar_cancellations': ['mean'],
                        'dp_delay': ['count', 'mean'],
                        'dp_cancellations': ['mean'],
                    }).compute()
            self.data = self.data.loc[~self.data.index.isna(), :]
            self.data = self.data.sort_index()
            self.data = add_rolling_mean(self.data, [('ar_delay', 'mean'),
                                                 ('ar_delay', 'count'),
                                                 ('ar_cancellations', 'mean'),
                                                 ('dp_delay', 'mean'),
                                                 ('dp_delay', 'count'),
                                                 ('dp_cancellations', 'mean')], window=5)
            self.data.to_csv(self.CACHE_PATH)

        self.plot = lambda: plot(self.data,
                                 title='Delay within one hour',
                                 x_label='Minute',
                                 y1_label='Stops',
                                 y2_label='Delay in minutes',
                                 formatter=mdates.DateFormatter("%M"),
                                 locator=mdates.MinuteLocator(byminute=range(0, 60, 10)))

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


class OverDay:
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
            rtd_df['daytime'] = rtd_df['ar_pt'].dt.time
            rtd_df['daytime'] = rtd_df['daytime'].fillna(value=rtd_df['dp_pt'].dt.time)
            rtd_df['daytime'] = rtd_df.map_partitions(self.daytime, meta=rtd_df['date_id'])
            rtd_df = rtd_df.loc[~rtd_df['daytime'].isna(), :]
            self.data = rtd_df.groupby('daytime').agg({
                        'ar_delay': ['count', 'mean'],
                        'ar_cancellations': ['mean'],
                        'dp_delay': ['count', 'mean'],
                        'dp_cancellations': ['mean'],
                    }).compute()
            self.data = self.data.loc[~self.data.index.isna(), :]
            self.data = self.data.sort_index()
            self.data = add_rolling_mean(self.data, [('ar_delay', 'mean'),
                                                 ('ar_delay', 'count'),
                                                 ('ar_cancellations', 'mean'),
                                                 ('dp_delay', 'mean'),
                                                 ('dp_delay', 'count'),
                                                 ('dp_cancellations', 'mean')], window=21)
            self.data.to_csv(self.CACHE_PATH)

        self.plot = lambda: plot(self.data,
                                 title='Delay within one day',
                                 x_label='Time',
                                 y1_label='Stops',
                                 y2_label='Delay in minutes',
                                 formatter=mdates.DateFormatter("%H:%M"),
                                 locator=mdates.HourLocator(),
                                 ax1_ylim_bottom=0)

    @staticmethod
    def daytime(df):
        """
        Create datetime from time.

        Parameters
        ----------
        df : pandas.DataFrame
            DataFrame with the columns daytime (datetime.time).

        Returns
        -------
        pd.Series
            daytime combined with basedate (03-01-2000).
        """
        return pd.Series([datetime.datetime(year=2000, month=1, day=3, hour=time.hour, minute=time.minute) for time in df['daytime']], index=df['daytime'].index)


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
            rtd_df['weekday'] = rtd_df['ar_pt'].dt.dayofweek
            rtd_df['weekday'] = rtd_df['weekday'].fillna(value=rtd_df['dp_pt'].dt.dayofweek)
            rtd_df['daytime'] = rtd_df['ar_pt'].dt.time
            rtd_df['daytime'] = rtd_df['daytime'].fillna(value=rtd_df['dp_pt'].dt.time)
            rtd_df['weektime'] = rtd_df.map_partitions(self.weektime, meta=rtd_df['daytime'])
            self.data = rtd_df.groupby(['weektime']).agg({
                        'ar_delay': ['count', 'mean'],
                        'ar_cancellations': ['mean'],
                        'dp_delay': ['count', 'mean'],
                        'dp_cancellations': ['mean'],
                    }).compute()
            self.data = self.data.loc[~self.data.index.isna(), :]
            self.data = self.data.sort_index()
            self.data = add_rolling_mean(self.data, [('ar_delay', 'mean'),
                                                 ('ar_delay', 'count'),
                                                 ('ar_cancellations', 'mean'),
                                                 ('dp_delay', 'mean'),
                                                 ('dp_delay', 'count'),
                                                 ('dp_cancellations', 'mean')], window=21)
            self.data.to_csv(self.CACHE_PATH)
        self.plot = lambda: plot(self.data,
                                 title='Delay within one week',
                                 x_label='Time',
                                 y1_label='Stops',
                                 y2_label='Delay in minutes',
                                 formatter=mdates.DateFormatter("%A %H:%M"), # E.g.: Monday 08:00
                                 locator=mdates.HourLocator(interval=8),
                                 ax1_ylim_bottom=0)

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
            rtd_df['floating_hour'] = rtd_df['floating_hour'].fillna(value=rtd_df['dp_pt'].dt.hour // 24 * 24)
            rtd_df['date'] = rtd_df['ar_pt'].dt.date
            rtd_df['date'] = rtd_df['date'].fillna(value=rtd_df['dp_pt'].dt.date)
            rtd_df['floating_yeartime'] = rtd_df.map_partitions(self.floating_yeartime, meta=rtd_df['ar_pt'])
            self.data = rtd_df.groupby(['floating_yeartime']).agg({
                        'ar_delay': ['count', 'mean'],
                        'ar_cancellations': ['mean'],
                        'dp_delay': ['count', 'mean'],
                        'dp_cancellations': ['mean'],
                    }).compute()
            self.data = self.data.loc[~self.data.index.isna(), :]
            self.data = self.data.sort_index()
            full_index = pd.date_range(start=self.data.index.min(), end=self.data.index.max())
            full_data = pd.DataFrame(index=full_index, columns=self.data.columns)
            full_data.loc[self.data.index, :] = self.data.loc[:, :]
            self.data = full_data.fillna(0)

            # Calculate rolling mean
            for col in [('ar_delay', 'mean'),
                        ('ar_delay', 'count'),
                        ('ar_cancellations', 'mean'),
                        ('dp_delay', 'mean'),
                        ('dp_delay', 'count'),
                        ('dp_cancellations', 'mean')]:
                new_col_name = (col[0], col[1] + '_rolling_mean')
                self.data[new_col_name] = self.data[col].rolling(3, center=True).mean()
            self.data.iloc[1:-1].to_csv(self.CACHE_PATH)
        
        self.plot = lambda: plot(self.data,
                                 title='Delay over the years',
                                 x_label='Time',
                                 y1_label='Stops',
                                 y2_label='Delay in minutes',
                                 formatter=mdates.DateFormatter("%D"),
                                 locator=mdates.HourLocator(interval=24*7),
                                 ax1_ylim_bottom=0,
                                 ax2_ylim_bottom=0)
        

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


if __name__ == '__main__':
    import fancy_print_tcp
    rtd_ray = RtdRay()
    rtd_df = rtd_ray.load_data(columns=['ar_pt',
                                        'dp_pt',
                                        'ar_delay',
                                        'ar_cancellations',
                                        'dp_delay',
                                        'dp_cancellations'])

    # lagecy code to plot older data
    # from data_analisys.delay import load_with_delay
    # rtd_df = load_with_delay(columns=['station', 'c', 'f'])

    # time = OverHour(rtd_df, use_cache=False)
    # time.plot()

    # time = OverDay(rtd_df, use_cache=True)
    # time.plot()

    # time = OverWeek(rtd_df, use_cache=True)
    # time.plot()

    # time = OverYear(rtd_df, use_cache=False)
    # time.plot()