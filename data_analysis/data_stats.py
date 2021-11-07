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

    def __init__(self):
        from helpers import RtdRay

        self._rtd_d = RtdRay().load_data(columns = ["dp_delay", "ar_delay", "dp_pt", "ar_pt", "ar_cs", "dp_cs"])
        
        max_date = self._rtd_d['ar_pt'].max().compute()
        min_date = max_date - timedelta(1)
        print("Last 24h from %s to %s" % (min_date, max_date))

        # Only now filter the data, since we didn't have the min_date and max_date before
        self._rtd_d_new = self._rtd_d.loc[
            ((self._rtd_d['ar_pt'] >= min_date) | (self._rtd_d['dp_pt'] >= min_date)) 
            & ((self._rtd_d['ar_pt'] < max_date) | (self._rtd_d['dp_pt'] < max_date))
        ]

    def generate_stats(self):
        self.stats["time"] = datetime.now().strftime("%d.%m.%Y %H:%M")

        self.stats["all"]["num_ar_data"] = int(self._rtd_d["ar_pt"].count().compute())
        self.stats["all"]["num_dp_data"] = int(self._rtd_d["dp_pt"].count().compute())

        self.stats["all"]["max_ar_delay"] = str(self._rtd_d["ar_delay"].max().compute())
        self.stats["all"]["max_dp_delay"] = str(self._rtd_d["dp_delay"].max().compute())
        self.stats["all"]["avg_ar_delay"] = str(round(self._rtd_d["ar_delay"].mean().compute(), 2))
        self.stats["all"]["avg_dp_delay"] = str(round(self._rtd_d["dp_delay"].mean().compute(), 2))

        self.stats["all"]["perc_ar_delay"] = str(round(( 1 - ((self._rtd_d["ar_delay"] < 6).sum() / self.stats["all"]["num_ar_data"] * 1.0).compute()) * 100, 2))
        self.stats["all"]["perc_dp_delay"] = str(round(( 1 - ((self._rtd_d["dp_delay"] < 6).sum() / self.stats["all"]["num_dp_data"] * 1.0).compute()) * 100, 2))
        self.stats["all"]["perc_ar_cancel"] = str(round(((self._rtd_d["ar_cs"] == "c").sum() / self.stats["all"]["num_ar_data"]).compute() * 100, 2))
        self.stats["all"]["perc_dp_cancel"] = str(round(((self._rtd_d["dp_cs"] == "c").sum() / self.stats["all"]["num_dp_data"]).compute() * 100, 2))

        self.stats["new"]["num_ar_data"] = int(self._rtd_d_new["ar_pt"].count().compute())
        self.stats["new"]["num_dp_data"] = int(self._rtd_d_new["dp_pt"].count().compute())

        self.stats["new"]["max_ar_delay"] = str(self._rtd_d_new["ar_delay"].max().compute())
        self.stats["new"]["max_dp_delay"] = str(self._rtd_d_new["dp_delay"].max().compute())
        self.stats["new"]["avg_ar_delay"] = str(round(self._rtd_d_new["ar_delay"].mean().compute(), 2))
        self.stats["new"]["avg_dp_delay"] = str(round(self._rtd_d_new["dp_delay"].mean().compute(), 2))

        self.stats["new"]["perc_ar_delay"] = str(round((1 - ((self._rtd_d_new["ar_delay"] < 6).sum() / self.stats["new"]["num_ar_data"] * 1.0).compute()) * 100, 2))
        self.stats["new"]["perc_dp_delay"] = str(round((1 - ((self._rtd_d_new["dp_delay"] < 6).sum() / self.stats["new"]["num_dp_data"] * 1.0).compute()) * 100, 2))
        self.stats["new"]["perc_ar_cancel"] = str(round(((self._rtd_d_new["ar_cs"] == "c").sum() / self.stats["new"]["num_ar_data"]).compute() * 100, 2))
        self.stats["new"]["perc_dp_cancel"] = str(round(((self._rtd_d_new["dp_cs"] == "c").sum() / self.stats["new"]["num_dp_data"]).compute() * 100, 2))

        return self.stats

    def save_stats(self, name = "stats.json"):
        import json
        from config import CACHE_PATH
        with open(f"{CACHE_PATH}/{name}", "w") as file:
            json.dump(self.stats, file, indent=4)


if __name__ == '__main__':
    stats = Stats(min_date=datetime(2021,2,15))
    data = stats.generate_stats()
    print(data)
    stats.save_stats()
