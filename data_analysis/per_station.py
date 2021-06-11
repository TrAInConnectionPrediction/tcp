import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import dask.dataframe as dd
import io
import numpy as np
from PIL import Image
import datetime
import matplotlib
import matplotlib.pyplot as plt
plt.style.use('dark_background')
import cartopy.crs as ccrs
import cartopy
# Configure cache path in kubernetes
if os.path.isdir("/usr/src/app/cache"):
    cartopy.config['data_dir'] = '/usr/src/app/cache'
# Cartopy requirements
# apt-get install libproj-dev proj-data proj-bin  
# apt-get install libgeos-dev 

from helpers import StationPhillip, RtdRay, groupby_index_to_flat
from database import cached_table_fetch
from config import CACHE_PATH


def image_to_webp(buffer: io.BytesIO, path: str) -> None:
    image = Image.open(buffer)
    image = image.convert('RGBA')
    image.save(path, 'webp')

def dark_fig_ax_germany(crs):
    # Bounding Box of Germany
    left = 5.67
    right = 15.64
    bottom = 47.06
    top = 55.06

    fig, ax = plt.subplots(subplot_kw = {'projection': crs})

    ax.set_extent((left, right, bottom, top))
    ax.coastlines()
    ax.add_feature(cartopy.feature.OCEAN, facecolor='#191a1a')
    ax.add_feature(cartopy.feature.LAND, facecolor='#343332', edgecolor='#5c5b5b')
    ax.add_feature(cartopy.feature.LAKES, facecolor='#191a1a', edgecolor='#5c5b5b')
    ax.add_feature(cartopy.feature.RIVERS, edgecolor='#343332')
    ax.add_feature(cartopy.feature.STATES, edgecolor='#444444')
    ax.add_feature(cartopy.feature.BORDERS, edgecolor='#5c5b5b')

    ax.set_axis_off()

    return fig, ax


class PerStationAnalysis(StationPhillip):
    DELAY_PLOT = {
        "count_1": "ar_delay",
        "count_2": "dp_delay",
        "color_value": "dp_delay",
    }

    CANCELLATIONS_PLOT = {
        "count_1": "ar_delay",
        "count_2": "dp_delay",
        "color_value": "dp_happened",
    }

    DATA_CACHE_PATH = CACHE_PATH + "/per_station_data.csv"

    def __init__(self, rtd_df, use_cache=True):
        super().__init__()
        try:
            if not use_cache:
                raise FileNotFoundError
            self.data = pd.read_csv(self.DATA_CACHE_PATH, header=[0, 1], index_col=0)
            print("using cached data")
        except FileNotFoundError:
            # Use dask Client to do groupby as the groupby is complex and scales well on local cluster.
            from dask.distributed import Client
            client = Client()

            # Group data by stations and calculatate the mean delay and the percentage of cacellations
            self.data = (
                rtd_df.groupby("station", sort=False)
                .agg({
                    "ar_delay": ["count", "mean"],
                    "ar_happened": ["mean"],
                    "dp_delay": ["count", "mean"],
                    "dp_happened": ["mean"],
                })
                .compute()
            )

            self.data.to_csv(self.DATA_CACHE_PATH)


    def plot(self, data_to_plot):
        self.fig, self.ax = dark_fig_ax_germany(crs=ccrs.Miller())

        self.cmap = matplotlib.colors.LinearSegmentedColormap.from_list(
            "", ["green", "yellow", "red"]
        )
        
        x = np.zeros(len(self.data.index))
        y = np.zeros(len(self.data.index))
        size = self.data.loc[:, [(data_to_plot["count_1"], "count")]].to_numpy() + self.data.loc[:, [(data_to_plot["count_2"], "count")]].to_numpy()
        color = self.data.loc[:, [(data_to_plot["color_value"], "mean")]].to_numpy()

        for i, station in enumerate(self.data.index):
            try:
                x[i], y[i] = self.get_location(name=station)
            except KeyError:
                x[i], y[i] = 0, 0

        size = (size / size.max()) * 70

        scatter = self.ax.scatter(
            x,
            y,
            vmin=0,
            vmax=5,
            c=color,
            cmap=self.cmap,
            s=size,
            alpha=0.5,
            zorder=10,
            transform=ccrs.PlateCarree()
        )

        self.cbar = self.fig.colorbar(scatter)
        self.cbar.solids.set_edgecolor("face")
        self.cbar.outline.set_linewidth(0)

        self.cbar.ax.get_yaxis().labelpad = 15
        self.cbar.ax.set_ylabel("Ø Verspätung in Minuten", rotation=270)
        
        plt.savefig('darkmap.png', dpi=300, transparent=False)

        image_to_webp('darkmap.png')

        plt.show()


class PerStationOverTime(StationPhillip):
    FREQ = "48H"
    DEFAULT_PLOTS = ["no data available", "default"]
    MAP_CRS = ccrs.Miller()

    def __init__(self, rtd, **kwargs):
        super().__init__(**kwargs)

        # The cache from an older version of this class on potentially older data should
        # not be used. Thus, we create a random version that is attached to the filenames
        # in the cache.
        self.version = f'{id(self):x}'

        self.data = cached_table_fetch(
            'per_station_over_time',
            table_generator=lambda: self.generate_data(rtd),
            push=True,
            **kwargs
        )

        # Setup Plot https://stackoverflow.com/questions/9401658/how-to-animate-a-scatter-plot
        self.fig, self.ax = dark_fig_ax_germany(crs=self.MAP_CRS)

        self.cmap = matplotlib.colors.LinearSegmentedColormap.from_list(
            "", ["green", "yellow", "red"]
        )

        self.sc = self.ax.scatter(
            np.zeros(1),
            np.zeros(1),
            c=np.zeros(1),
            s=np.zeros(1),
            cmap=self.cmap,
            vmin=0,
            vmax=7,
            alpha=0.5,
            zorder=10,
            transform=ccrs.PlateCarree()
        )

        self.colorbar = self.fig.colorbar(self.sc)
        self.colorbar.solids.set_edgecolor("face")
        self.colorbar.outline.set_linewidth(0)

        self.colorbar.ax.get_yaxis().labelpad = 15
        self.colorbar.ax.set_ylabel("Ø Verspätung in Minuten", rotation=270)

        for plot_name in self.DEFAULT_PLOTS:
            if not os.path.isfile(f"{CACHE_PATH}/plot_cache/{self.version}_{plot_name}.webp"):
                if plot_name == 'default':
                    self.ax.set_title('', fontsize=16)
                else:
                    self.ax.set_title(plot_name, fontsize=16)
                memory_buffer = io.BytesIO()
                self.fig.savefig(memory_buffer, dpi=300, transparent=True)
                image_to_webp(memory_buffer, f"{CACHE_PATH}/plot_cache/{self.version}_{plot_name}.webp")

    def generate_data(self, rtd: dd.DataFrame) -> pd.DataFrame:
        # Use dask Client to do groupby as the groupby is complex and scales well on local cluster.
        from dask.distributed import Client
        client = Client()

        # Generate an index with self.FREQ for groupby over time and station
        rtd["stop_hour"] = rtd["ar_pt"].fillna(value=rtd["dp_pt"]).dt.round(self.FREQ)
        rtd = rtd.drop(columns=['ar_pt', 'dp_pt'])
        rtd["single_index_for_groupby"] = rtd["stop_hour"].astype("str") + rtd[
            "station"
        ].astype("str")

        data: pd.DataFrame = (
            rtd.groupby("single_index_for_groupby", sort=False)
            .agg({
                "ar_delay": ["mean"],
                "ar_happened": ["sum"],
                "dp_delay": ["mean"],
                "dp_happened": ["sum"],
                "stop_hour": ["first"],
                "station": ["first"],
                "lat": ['first'],
                "lon": ['first'],
            })
            .compute()
        )

        data = groupby_index_to_flat(data)
        return data

    def limits(self):
        return {
            "min": self.data["stop_hour"].min(),
            "max": self.data["stop_hour"].max()
        }

    def generate_plot(self, start_time, end_time, use_cached_images=False) -> str:
        """
        Generates a plot that visualizes all the delays on a Germany map between `start_time` and `end_time`
        The file is generated relative to this execution path inside of  `cache/plot_cache/{plot_name}.webp`

        Parameters
        ----------
        start_time : datetime.datetime
            Start of time range
        end_time : datetime.datetime
            End of time range

        Returns
        -------
        str
            Path to the generated plot
        """

        if start_time + datetime.timedelta(hours=48) > end_time:
            # We generate plots over a minimum timespan of 48 hours
            end_time = end_time + datetime.timedelta(hours=48)

        plot_name = (
            start_time.strftime("%d.%m.%Y")
            + "-"
            + end_time.strftime("%d.%m.%Y")
        )

        plot_path = f"{CACHE_PATH}/plot_cache/{self.version}_{plot_name}.webp"

        if use_cached_images and os.path.isfile(plot_path):
            # Return cached image
            return plot_path

        # Extract data that is between start_time and end_time
        current_data = self.data.loc[
            (start_time <= self.data["stop_hour"])
            & (self.data["stop_hour"] < end_time)
        ].copy()

        if not current_data.empty:
            # As self.data is already preaggregated we need to compute the weighted
            # mean of the delays. This requires several steps with pandas.
            # Get the number of datapoints in each preaggregated datapoint
            group_sizes = current_data.groupby("station").agg(
                {
                    "ar_happened_sum": "sum",
                    "dp_happened_sum": "sum",
                }
            )
            # For each preaggregated datapoint of each station, calculate its fraction of stops
            # compared to the total stops at the station
            group_sizes = current_data.set_index('station')[['ar_happened_sum', 'dp_happened_sum']] / group_sizes
            # rename columns in order to mulptiply them with the mean delays
            group_sizes.rename(columns={'ar_happened_sum': 'ar_delay_mean', 'dp_happened_sum': 'dp_delay_mean'}, inplace=True)
            group_sizes.reset_index(drop=True, inplace=True)
            # calculate the minutes of delay from each preaggregated datapoint of each station
            weighted_mean = (current_data.reset_index()[['ar_delay_mean', 'dp_delay_mean']] * group_sizes)
            weighted_mean.index = current_data.index
            # re-insert it into the original preagregated datapoint in order to aggregate it
            current_data.loc[:, ['ar_delay_mean', 'dp_delay_mean']] = weighted_mean[['ar_delay_mean', 'dp_delay_mean']]

            current_data = current_data.groupby("station").agg(
                {
                    "ar_delay_mean": "sum",
                    "ar_happened_sum": "sum",
                    "dp_delay_mean": "sum",
                    "dp_happened_sum": "sum",
                    "lat": "first",
                    "lon": "first",
                }
            )
            current_data = current_data.fillna(0)

            size = (
                current_data.loc[:, ["ar_happened_sum"]].to_numpy()[:, 0]
                + current_data.loc[:, ["dp_happened_sum"]].to_numpy()[:, 0]
            )
            size = (size / size.max()) * 70

            color = current_data.loc[:, ["ar_delay_mean"]].to_numpy().astype(float)[:, 0]            

            # change the positions 
            self.sc.set_offsets(np.c_[current_data['lon'], current_data['lat']])
            # change the sizes
            self.sc.set_sizes(size)
            # change the color
            self.sc.set_array(color)

            self.ax.set_title(plot_name.replace("_", ":").replace('-', ' - '), fontsize=12)
            memory_buffer = io.BytesIO()
            self.fig.savefig(memory_buffer, dpi=300, transparent=True)
            image_to_webp(memory_buffer, plot_path)
        else:
            plot_path = f"{CACHE_PATH}/plot_cache/{self.version}_no data available.webp"

        return plot_path


if __name__ == "__main__":
    import helpers.fancy_print_tcp

    rtd_df=None
    rtd_ray = RtdRay()
    rtd_df = rtd_ray.load_data(
        columns=[
            "ar_pt",
            "dp_pt",
            "station",
            "ar_delay",
            "ar_happened",
            "dp_delay",
            "dp_happened",
            "lat",
            "lon",
        ],
        min_date=datetime.datetime(2021, 3, 1)
    )

    # per_station = PerStationAnalysis(rtd_df, use_cache=True)
    # per_station.plot(per_station.DELAY_PLOT)

    per_station_time = PerStationOverTime(rtd_df, generate=False, prefer_cache=True)
    per_station_time.generate_plot(
        datetime.datetime(2021, 3, 1, hour=0), datetime.datetime(2021, 3, 10, hour=0)
    )
    per_station_time.generate_plot(
        datetime.datetime(2021, 3, 10, hour=0), datetime.datetime(2021, 3, 20, hour=0)
    )
