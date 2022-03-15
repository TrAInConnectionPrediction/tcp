import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

if os.path.isfile("/mnt/config/config.py"):
    sys.path.append("/mnt/config/")

# Print Logo
import helpers.bahn_vorhersage

if __name__ == '__main__':

    print("Init")

    from dask.distributed import Client
    with Client(n_workers=2, threads_per_worker=1, memory_limit='16GB') as client:
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

    from data_analysis import data_stats

    data_stats.load_stats(
        table_generator=data_stats.stats_generator,
        generate=True,
    )

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
