import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Print Logo
import helpers.fancy_print_tcp

print("Init")

if os.path.isfile("/mnt/config/config.py"):
    sys.path.append("/mnt/config/")
import config

# load/import classes
from dask.distributed import Client

# set up cluster and workers
client = Client(ip='127.0.0.1', n_workers=4, threads_per_worker=2, memory_limit='16GB')

from helpers import RtdRay
rtd_ray = RtdRay()

print("Done")

print("Refreshing local Cache...")
# If this doesn't work properly switch to 
# TODO switch to rtd_ray.upgrade_rtd()
rtd_ray.download_rtd()
# rtd_ray.upgrade_rtd()

print("Done")

print("Generating Statistics...")

print("--Overview")

from data_analysis.data_stats import Stats
stats = Stats()
stats.generate_stats()
stats.save_stats()

print("--Done")

print("--Per Station Data")

import datetime
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
    min_date=datetime.datetime(2021, 1, 1)
)

from data_analysis.per_station import PerStationOverTime
PerStationOverTime(rtd_df, generate=True, use_cache=False)

# del rtd_df
print("--Done")

print("Training ML Models...")

# TODO

print("Done")