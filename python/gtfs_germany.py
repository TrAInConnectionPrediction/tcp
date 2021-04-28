# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
from IPython import get_ipython

# %%
import sys
from pathlib import Path
import json

import pandas as pd
import numpy as np
import geopandas as gp
import matplotlib

DIR = Path('..')
sys.path.append(str(DIR))

import gtfs_kit as gk

DATA_DIR = DIR/'data/'

get_ipython().run_line_magic('load_ext', 'autoreload')
get_ipython().run_line_magic('autoreload', '2')
get_ipython().run_line_magic('matplotlib', 'inline')


# %%
# List feed

path = DATA_DIR/'gtfs-germany.zip'
gk.list_feed(path)


# %%
# Read feed and describe

path = DATA_DIR/'gtfs-germany.zip'
feed = gk.read_feed(path, dist_units='km')
feed.describe()


# %%
# Validate

feed.validate()


# %%
# Append shape_dist_traveled column to stop times

display(feed.stop_times.head().T)

feed = feed.append_dist_to_stop_times()
feed.stop_times.head().T


# %%
# Choose study dates

week = feed.get_first_week()
dates = [week[4], week[6]]  # First Friday and Sunday
dates


# %%
# Compute feed time series

trip_stats = feed.compute_trip_stats()
trip_stats.head().T
fts = feed.compute_feed_time_series(trip_stats, dates, freq='6H')
fts


# %%
gk.downsample(fts, freq='12H')


# %%
# Compute feed stats for first week

feed_stats = feed.compute_feed_stats(trip_stats, week)
feed_stats


# %%
# Compute route time series

rts = feed.compute_route_time_series(trip_stats, dates, freq='12H')
rts


# %%
# Slice time series

inds = ['service_distance', 'service_duration', 'service_speed']
rids = ['110-423', '110N-423']

rts.loc[:, (inds, rids)]


# %%
# Slice again by cross-section

rts.xs(rids[0], axis="columns", level=1)


# %%
# Compute trip locations for every hour

rng = pd.date_range('1/1/2000', periods=24, freq='H')
times = [t.strftime('%H:%M:%S') for t in rng]
loc = feed.locate_trips(dates[0], times)
loc.head()


# %%
# Build a route timetable

route_id = feed.routes['route_id'].iat[0]
feed.build_route_timetable(route_id, dates).T


# %%
# Compute screen line counts

path = DATA_DIR/'cairns_screen_lines.geojson'
lines = gp.read_file(path)

display(lines)

feed.compute_screen_line_counts(lines, dates)


# %%
# Map routes

rids = feed.routes.route_id.loc[2:4]
feed.map_routes(rids, include_stops=True)
