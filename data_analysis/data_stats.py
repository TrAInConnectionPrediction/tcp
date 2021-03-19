import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timedelta


class Stats:
    # When gernerated
    # Number of datapoints
    # Number of new datapoints in the last day
    # Biggest delay all time
    # Biggest delay yesterday
    # Average delay all time
    # Average delay yesterday
    # Percent of trains on time all time (< 6 min)
    # Percent of trains on time yesterday (< 6 min)
    # Percent of cancelt trains all Time
    # Percent of cancelt trains yesterday

    # Last day Models on yesterdays data
    # Graph?
    stats = {'all': {}, 'new': {}}

    def __init__(self, old_stats={}, min_date=datetime.now() - timedelta(1)):
        from helpers.RtdRay import RtdRay

        self._rtd_d = RtdRay().load_data(columns = ["dp_delay", "ar_delay", "dp_pt", "ar_pt", "ar_cs", "dp_cs"])
        self._rtd_d_new = self._rtd_d.loc[
            (self._rtd_d["ar_pt"] >= min_date) | (self._rtd_d["dp_pt"] >= min_date)
        ]

        self._old_stats = old_stats

    def generate_stats(self):
        self.stats["time"] = datetime.now().strftime("%d.%m.%Y %H:%M")

        self.stats["all"]["num_ar_data"] = int(self._rtd_d["ar_pt"].count().compute())
        self.stats["all"]["num_dp_data"] = int(self._rtd_d["dp_pt"].count().compute())

        self.stats["all"]["max_ar_delay"] = self._rtd_d["ar_delay"].max().compute()
        self.stats["all"]["max_dp_delay"] = self._rtd_d["dp_delay"].max().compute()
        self.stats["all"]["avg_ar_delay"] = self._rtd_d["ar_delay"].mean().compute()
        self.stats["all"]["avg_dp_delay"] = self._rtd_d["dp_delay"].mean().compute()

        self.stats["all"]["perc_ar_delay"] = ((self._rtd_d["ar_delay"] < 6).sum() / self.stats["all"]["num_ar_data"]).compute()
        self.stats["all"]["perc_dp_delay"] = ((self._rtd_d["dp_delay"] < 6).sum() / self.stats["all"]["num_dp_data"]).compute()
        self.stats["all"]["perc_ar_cancel"] = ((self._rtd_d["ar_cs"] == "c").sum() / self.stats["all"]["num_ar_data"]).compute()
        self.stats["all"]["perc_dp_cancel"] = ((self._rtd_d["dp_cs"] == "c").sum() / self.stats["all"]["num_dp_data"]).compute()

        self.stats["new"]["num_ar_data"] = int(self._rtd_d_new["ar_pt"].count().compute())
        self.stats["new"]["num_dp_data"] = int(self._rtd_d_new["dp_pt"].count().compute())

        self.stats["new"]["max_ar_delay"] = self._rtd_d_new["ar_delay"].max().compute()
        self.stats["new"]["max_dp_delay"] = self._rtd_d_new["dp_delay"].max().compute()
        self.stats["new"]["avg_ar_delay"] = self._rtd_d_new["ar_delay"].mean().compute()
        self.stats["new"]["avg_dp_delay"] = self._rtd_d_new["dp_delay"].mean().compute()

        self.stats["new"]["perc_ar_delay"] = ((self._rtd_d_new["ar_delay"] < 6).sum() / self.stats["new"]["num_ar_data"]).compute()
        self.stats["new"]["perc_dp_delay"] = ((self._rtd_d_new["dp_delay"] < 6).sum() / self.stats["new"]["num_dp_data"]).compute()
        self.stats["new"]["perc_ar_cancel"] = ((self._rtd_d_new["ar_cs"] == "c").sum() / self.stats["new"]["num_ar_data"]).compute()
        self.stats["new"]["perc_dp_cancel"] = ((self._rtd_d_new["dp_cs"] == "c").sum() / self.stats["new"]["num_dp_data"]).compute()

        return self.stats


if __name__ == '__main__':
    stats = Stats(min_date=datetime(2021,2,15))
    data = stats.generate_stats()
    print(data)
    import json
    from config import CACHE_PATH
    with open(f"{CACHE_PATH}/stats.json", "w") as file:
        json.dump(data, file, indent=4)
