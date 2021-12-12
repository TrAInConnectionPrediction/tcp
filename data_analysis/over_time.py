import os, sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from abc import ABCMeta, abstractmethod
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker
import datetime
import dask.dataframe as dd
from helpers import RtdRay
from config import n_dask_workers
from database import cached_table_fetch
from helpers import groupby_index_to_flat


def add_rolling_mean(df: pd.DataFrame, columns: list, window=3) -> pd.DataFrame:
    """
    Add rolling mean to periodic data.

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
        df.loc[df.index[0] - df.index[i + 1] + df.index[0], :] = df.iloc[max_index - i, :]
        df.loc[df.index[max_index] + (df.index[i + 1] - df.index[0]), :] = df.iloc[i, :]
    df = df.sort_index()

    # Calculate rolling mean on columns
    for col in columns:
        df[col] = df[col].rolling(window, center=True).mean()
    return df.iloc[dist_from_center:-dist_from_center]


class OverTime(metaclass=ABCMeta):
    def __init__(self, **kwargs):
        self.data = cached_table_fetch(
            self.tablename,
            table_generator=self.generate_data,
            push=True,
            index_col='index',
            **kwargs
        )

    @property
    @abstractmethod
    def tablename(self):
        pass

    @property
    @abstractmethod
    def resolution(self):
        pass

    @property
    @abstractmethod
    def rolling_mean_window(self):
        pass

    @abstractmethod
    def periodic_aggregator(self, rtd: dd.Series) -> dd.Series:
        pass

    @property
    @abstractmethod
    def dt_formatter(self):
        pass

    @property
    @abstractmethod
    def dt_locator(self):
        pass

    @property
    @abstractmethod
    def plot_title(self):
        pass

    @property
    @abstractmethod
    def plot_x_label(self):
        pass

    def generate_data(self):
        rtd = RtdRay.load_data(
            columns=[
                'ar_pt',
                'dp_pt',
                'ar_delay',
                'ar_happened',
                'dp_delay',
                'dp_happened',
                'ar_cs',
                'dp_cs',
            ]
        )
    
        rtd['ar_on_time'] = rtd['ar_delay'] <= 5
        rtd['dp_on_time'] = rtd['dp_delay'] <= 5
        rtd['ar_canceled'] = rtd['ar_cs'] == 'c'
        rtd['dp_canceled'] = rtd['dp_cs'] == 'c'

        # Use dask Client to do groupby as the groupby is complex and scales well on local cluster.
        from dask.distributed import Client
        with Client(n_workers=n_dask_workers, threads_per_worker=2) as client:
            rtd['agg_time'] = rtd['ar_pt'].fillna(value=rtd['dp_pt'])
            rtd['agg_time'] = rtd['agg_time'].dt.floor(self.resolution)
            rtd['agg_time'] = self.periodic_aggregator(rtd['agg_time'])

            rtd = rtd.groupby('agg_time').agg({
                'ar_delay': ['count', 'mean'],
                'ar_happened': ['mean', 'sum'],
                'ar_on_time': ['mean'],
                'ar_canceled': ['mean'],
                'dp_delay': ['count', 'mean'],
                'dp_happened': ['mean', 'sum'],
                'dp_on_time': ['mean'],
                'dp_canceled': ['mean'],
            }).compute()
            rtd = rtd.loc[~rtd.index.isna(), :]
            rtd = rtd.sort_index()
            rtd = groupby_index_to_flat(rtd)

            rtd = rtd.astype({
                'ar_delay_mean': 'float64',
                'ar_delay_count': 'int',
                'ar_happened_mean': 'float64',
                'ar_happened_sum': 'int',
                'ar_on_time_mean': 'float64',
                'ar_canceled_mean': 'float64',
                'dp_delay_mean': 'float64',
                'dp_delay_count': 'int',
                'dp_happened_mean': 'float64',
                'dp_happened_sum': 'int',
                'dp_on_time_mean': 'float64',
                'dp_canceled_mean': 'float64',
            })

            if self.rolling_mean_window > 0:
                rtd = add_rolling_mean(
                    rtd, [
                        'ar_delay_mean',
                        'ar_delay_count',
                        'ar_happened_mean',
                        'ar_happened_sum',
                        'ar_on_time_mean',
                        'ar_canceled_mean',
                        'dp_delay_mean',
                        'dp_delay_count',
                        'dp_happened_mean',
                        'dp_happened_sum',
                        'dp_on_time_mean',
                        'dp_canceled_mean',
                    ],
                    window=self.rolling_mean_window
                )

            return rtd

    def plot(
            self,
            nticks=6,
            kind: str = 'delay'
    ):
        if kind not in ['delay', 'cancellations', 'on_time_percentage']:
            raise ValueError(f'kind must be "delay", "cancellations" or "on_time_percentage" not {kind}')
        fig, ax1 = plt.subplots()
        ax2 = ax1.twinx()
        ax1.tick_params(axis="both", labelsize=20)
        ax2.tick_params(axis="both", labelsize=20)
        ax1.xaxis.set_major_formatter(self.dt_formatter)
        ax1.xaxis.set_major_locator(self.dt_locator)
        ax2.xaxis.set_major_formatter(self.dt_formatter)
        ax2.xaxis.set_major_locator(self.dt_locator)
        dt_index = self.data.index.to_numpy()
        ax1.set_xlim(dt_index.min(), dt_index.max())

        ax1.grid(True)
        ax2.grid(True)

        # Align grids of ax1 and ax2
        ax1.yaxis.set_major_locator(matplotlib.ticker.LinearLocator(nticks))
        ax2.yaxis.set_major_locator(matplotlib.ticker.LinearLocator(nticks))

        ax1.set_title(self.plot_title, fontsize=50)
        ax1.set_xlabel(self.plot_x_label, fontsize=30)
        ax1.set_ylabel('Stops', color="blue", fontsize=30)

        if kind == 'delay':
            ax1.plot(self.data['ar_happened_sum'] + self.data['dp_happened_sum'],
                     color="blue",
                     linewidth=3,
                     label='Stops')

            ax2.set_ylabel('Delay in Minutes', color="orange", fontsize=30)
            ax2.plot(self.data['ar_delay_mean'],
                     color="red",
                     linewidth=3,
                     label='Arrival delay')
            ax2.plot(self.data['dp_delay_mean'],
                     color="orange",
                     linewidth=3,
                     label='Departure delay')
        elif kind == 'on_time_percentage':
            ax1.plot(self.data['ar_happened_sum'] + self.data['dp_happened_sum'],
                     color="blue",
                     linewidth=3,
                     label='Stops')

            ax2.set_ylabel('Stops on time (percent)', color="orange", fontsize=30)
            ax2.plot((self.data['ar_on_time_mean']).round(3) * 100,
                     color="red",
                     linewidth=3,
                     label='Arrival')
            ax2.plot((self.data['dp_on_time_mean']).round(3) * 100,
                     color="orange",
                     linewidth=3,
                     label='Departure')
            ax2.set_ylim(75, 100)

        elif kind == 'cancellations':
            ax1.plot(self.data['ar_happened_sum'] + self.data['dp_happened_sum'],
                     color="blue",
                     linewidth=3,
                     label='Stops')

            ax2.set_ylabel('Relative non-cancellations', color="orange", fontsize=30)
            ax2.plot(self.data['ar_happened_mean'],
                     color="red",
                     linewidth=3,
                     label='Arrival non-cancellations')
            ax2.plot(self.data['dp_happened_mean'],
                     color="orange",
                     linewidth=3,
                     label='Departure non-cancellations')

        fig.legend(fontsize=18)
        fig.autofmt_xdate()
        plt.show()


class OverHour(OverTime):
    tablename = 'over_hour'
    resolution = '1min'
    rolling_mean_window = 3
    dt_formatter = mdates.DateFormatter("%M")
    dt_locator = mdates.MinuteLocator(byminute=range(0, 60, 10))
    plot_title = 'Stops over the hour'
    plot_x_label = 'Minute'

    def periodic_aggregator(self, rtd: dd.Series) -> dd.Series:
        return (
            datetime.datetime(year=2000, month=1, day=3)
            + dd.to_timedelta(rtd.dt.minute, unit='m')
        )


class OverDay(OverTime):
    tablename = 'over_day'
    resolution = '10min'
    rolling_mean_window = 5
    dt_formatter = mdates.DateFormatter("%H:%M")
    dt_locator = mdates.HourLocator()
    plot_title = 'Stops over the day'
    plot_x_label = 'Time'

    def periodic_aggregator(self, rtd: dd.Series) -> dd.Series:
        return (
            datetime.datetime(year=2000, month=1, day=3)
            + dd.to_timedelta(rtd.dt.hour, unit='h')
            + dd.to_timedelta(rtd.dt.minute, unit='m')
        )


class OverWeek(OverTime):
    tablename = 'over_week'
    resolution = '10min'
    rolling_mean_window = 5
    dt_formatter = mdates.DateFormatter("%A %H:%M")  # E.g.: Monday 08:00
    dt_locator = mdates.HourLocator(interval=12)
    plot_title = 'Stops over the week'
    plot_x_label = 'Day'

    def periodic_aggregator(self, rtd: dd.Series) -> dd.Series:
        return (
            datetime.datetime(year=2000, month=1, day=3)
            + dd.to_timedelta(rtd.dt.dayofweek, unit='d')
            + dd.to_timedelta(rtd.dt.hour, unit='h')
            + dd.to_timedelta(rtd.dt.minute, unit='m')
        )


class OverYear(OverTime):
    tablename = 'overyear'
    resolution = '1D'
    rolling_mean_window = 0
    dt_formatter = mdates.DateFormatter("%b %Y")
    dt_locator = mdates.MonthLocator()
    plot_title = 'Stops over the year'
    plot_x_label = 'Month'

    def periodic_aggregator(self, rtd: dd.Series) -> dd.Series:
        return rtd


class OverYearWeekly(OverTime):
    tablename = 'over_year_weekly'
    resolution = '7D'
    rolling_mean_window = 0
    dt_formatter = mdates.DateFormatter("%b %Y")
    dt_locator = mdates.MonthLocator()
    plot_title = 'Stops over the year'
    plot_x_label = 'Month'

    def periodic_aggregator(self, rtd: dd.Series) -> dd.Series:
        return rtd


if __name__ == '__main__':
    import helpers.fancy_print_tcp

    rtd_df = RtdRay.load_data(
        columns=[
            'ar_pt',
            'dp_pt',
            'ar_delay',
            'ar_happened',
            'dp_delay',
            'dp_happened',
            'ar_cs',
            'dp_cs',
        ]
    )

    rtd_df['ar_on_time'] = rtd_df['ar_delay'] <= 5
    rtd_df['dp_on_time'] = rtd_df['dp_delay'] <= 5
    rtd_df['ar_canceled'] = rtd_df['ar_cs'] == 'c'
    rtd_df['dp_canceled'] = rtd_df['dp_cs'] == 'c'

    print('grouping over hour')
    time = OverHour(generate=False)
    time.plot()

    print('grouping over day')
    time = OverDay(generate=False)
    time.plot()

    print('grouping over week')
    time = OverWeek(generate=False)
    time.plot()

    print('grouping over year')
    time = OverYear(generate=False)
    time.plot(kind='on_time_percentage')

    print('grouping over year')
    time = OverYearWeekly(generate=False)
    time.plot(kind='on_time_percentage')
