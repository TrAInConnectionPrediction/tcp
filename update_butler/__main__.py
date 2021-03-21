import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# Print Logo
import helpers.fancy_print_tcp

print("Init")

from helpers.RtdRay import RtdRay
rtd_ray = RtdRay()

print("Done")

print("Parsing new data...")

# TODO

print("Done")

print("Refreshing local Cache...")
# If this doesn't work properly switch to 
# TODO switch to rtd_ray.update_local_buffer()
rtd_ray.refresh_local_buffer()

print("Done")

print("Generating Statistics...")

print("--Overview")

from data_analysis.data_stats import Stats
stats = Stats()
stats.generate_stats()
stats.save_stats()

print("--Done")

print("--Per Station Data")

rtd_df = rtd_ray.load_data(
    columns=[
        "ar_pt",
        "dp_pt",
        "station",
        "ar_delay",
        "ar_cancellations",
        "dp_delay",
        "dp_cancellations",
    ]
)

from data_analysis.per_station import PerStationOverTime
PerStationOverTime(rtd_df, use_cache=False)

del rtd_df
print("--Done")

print("Training ML Models...")

# TODO

print("Done")