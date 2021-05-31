import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import matplotlib.pyplot as plt
from helpers import RtdRay
from config import CACHE_PATH


class DelayAnalysis:
    def __init__(self, rtd_df, use_cache=True):
        self.CACHE_PATH = f'{CACHE_PATH}/delay_analysis.csv'

        try:
            if not use_cache:
                raise FileNotFoundError
            self.data = pd.read_csv(self.CACHE_PATH, header=[0, 1], index_col=0, parse_dates=[0])
            print('using cached data')
        except FileNotFoundError:
            # Use dask Client to do groupby as the groupby is complex and scales well on local cluster.
            from dask.distributed import Client
            client = Client()

            self.data = rtd_df.groupby('ar_delay').agg({
                        'ar_pt': ['count'],
                        'ar_happened': ['sum'],
                        'dp_pt': ['count'],
                        'dp_happened': ['sum'],
                    }).compute()
            self.data = self.data.nlargest(50, columns=('ar_pt', 'count'))
            self.data = self.data.sort_index()
            self.data.to_csv(self.CACHE_PATH)

    def plot_count(self):
        fig, ax1 = plt.subplots()
        ax1.tick_params(axis="both", labelsize=20) 
        index = self.data.index.to_numpy()
        ax1.set_xlim(index.min(), index.max())
        ax1.set_yscale('log')

        ax1.grid(True)

        ax1.set_title('Delay distribution', fontsize=50)
        ax1.set_xlabel('Delay in minutes', fontsize=30)
        ax1.set_ylabel('Count', color="blue", fontsize=30)

        ax1.plot(
            self.data[('ar_happened', 'sum')] + self.data[('ar_happened', 'sum')],
            color="blue",
            linewidth=3,
            label='Stops',
        )

        ax1.plot(
            (self.data[('ar_pt', 'count')]
            + self.data[('dp_pt', 'count')]
            - self.data[('ar_happened', 'sum')]
            - self.data[('ar_happened', 'sum')]),
            color="orange",
            linewidth=3,
            label='Cancellations',
        )             
        
        fig.legend(fontsize=20)
        ax1.set_ylim(bottom=0)
        plt.show()


if __name__ == '__main__':
    import helpers.fancy_print_tcp

    rtd_ray = RtdRay()
    rtd = rtd_ray.load_data(columns=['ar_delay', 'dp_delay', 'ar_pt', 'dp_pt', 'ar_happened', 'dp_happened'])
    delay = DelayAnalysis(rtd, use_cache=True)
    delay.plot_count()