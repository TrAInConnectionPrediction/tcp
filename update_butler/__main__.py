import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Print Logo
import helpers.fancy_print_tcp

print("Init")

# load/import classes
from dask.distributed import Client

# set up cluster and workers
client = Client(ip='127.0.0.1', n_workers=10, threads_per_worker=2, memory_limit='16GB')

from helpers import RtdRay

print("Done")

print("Refreshing local Cache...")
# If this doesn't work properly switch to 
# TODO switch to RtdRay.upgrade_rtd()
RtdRay.download_rtd()
# RtdRay.upgrade_rtd()

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
rtd_df = RtdRay.load_data(
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
