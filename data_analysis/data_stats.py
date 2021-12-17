import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import datetime
import pandas as pd
from helpers import RtdRay, lru_cache_time
from database import cached_table_fetch
from config import n_dask_workers


def stats_generator() -> pd.DataFrame:
    print("Generating stats...")
    from dask.distributed import Client
    with Client(n_workers=n_dask_workers, threads_per_worker=2) as client:
        rtd = RtdRay.load_data(
            columns=["dp_delay", "ar_delay", "dp_pt", "ar_pt", "ar_cs", "dp_cs", "ar_happened", "dp_happened"],
        )

        stats = {}

        stats["all_num_ar_data"] = int(rtd["ar_happened"].sum().compute())
        stats["all_num_dp_data"] = int(rtd["dp_happened"].sum().compute())
        stats["all_num_ar_cancel"] = int((rtd["ar_cs"] == "c").sum().compute())
        stats["all_num_dp_cancel"] = int((rtd["dp_cs"] == "c").sum().compute())

        stats["all_max_ar_delay"] = int(rtd["ar_delay"].max().compute())
        stats["all_max_dp_delay"] = int(rtd["dp_delay"].max().compute())
        stats["all_avg_ar_delay"] = float(round(rtd["ar_delay"].mean().compute(), 2))
        stats["all_avg_dp_delay"] = float(round(rtd["dp_delay"].mean().compute(), 2))

        stats["all_perc_ar_delay"] = float(round((((rtd["ar_delay"] > 5).sum() / (stats["all_num_ar_data"])).compute()) * 100, 2))
        stats["all_perc_dp_delay"] = float(round((((rtd["dp_delay"] > 5).sum() / (stats["all_num_dp_data"])).compute()) * 100, 2))
        stats["all_perc_ar_cancel"] = float(round((stats["all_num_ar_cancel"] / (stats["all_num_ar_data"] + stats["all_num_ar_cancel"])) * 100, 2))
        stats["all_perc_dp_cancel"] = float(round((stats["all_num_dp_cancel"] / (stats["all_num_dp_data"] + stats["all_num_dp_cancel"])) * 100, 2))

        stats["time"] = datetime.datetime.today().strftime("%d.%m.%Y %H:%M")

        today = datetime.datetime.combine(datetime.datetime.today().date(), datetime.time())
        yersterday = today - datetime.timedelta(days=1)

        # One day will alsways fit into ram, so we compute the loaded dask DataFrame right away
        rtd = RtdRay.load_data(
            columns=["dp_delay", "ar_delay", "dp_pt", "ar_pt", "ar_cs", "dp_cs", "ar_happened", "dp_happened"],
            min_date=yersterday,
            max_date=today,
        ).compute()

        
        stats["new_date"] = yersterday.strftime("%d.%m.%Y")
        stats["new_num_ar_data"] = int(rtd["ar_happened"].sum())
        stats["new_num_dp_data"] = int(rtd["dp_happened"].sum())
        stats["new_num_ar_cancel"] = int((rtd["ar_cs"] == "c").sum())
        stats["new_num_dp_cancel"] = int((rtd["dp_cs"] == "c").sum())

        stats["new_max_ar_delay"] = int(rtd["ar_delay"].max())
        stats["new_max_dp_delay"] = int(rtd["dp_delay"].max())
        stats["new_avg_ar_delay"] = float(round(rtd["ar_delay"].mean(), 2))
        stats["new_avg_dp_delay"] = float(round(rtd["dp_delay"].mean(), 2))

        stats["new_perc_ar_delay"] = float(round((((rtd["ar_delay"] > 5).sum() / (stats["new_num_ar_data"]))) * 100, 2))
        stats["new_perc_dp_delay"] = float(round((((rtd["dp_delay"] > 5).sum() / (stats["new_num_dp_data"]))) * 100, 2))
        stats["new_perc_ar_cancel"] = float(round((stats["new_num_ar_cancel"] / (stats["new_num_ar_data"] + stats["new_num_ar_cancel"])) * 100, 2))
        stats["new_perc_dp_cancel"] = float(round((stats["new_num_dp_cancel"] / (stats["new_num_dp_data"] + stats["new_num_dp_cancel"])) * 100, 2))

        return pd.DataFrame({key: [stats[key]] for key in stats})

@lru_cache_time(maxsize=1, time_to_last=60*60)
def load_stats(**kwargs) -> dict:
    """Loads stats from database or local

    Args:
        **kwargs: passed to `cached_table_fetch`. See its docstring for more info.

    Returns
    -------
    dict
        Loaded stats
    """
    stats = cached_table_fetch(
        "stats_overview",
        **kwargs
    )

    return stats.iloc[0].to_dict()


if __name__ == '__main__':
    stats = load_stats(
        table_generator=stats_generator,
        generate=True,
    )

    print(stats)