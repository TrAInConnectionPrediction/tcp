import os
import sys
import abc
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
from data_analysis.packed_bubbles import BubbleChart
from helpers.RtdRay import RtdRay
from helpers import groupby_index_to_flat
from database.cached_table_fetch import cached_table_fetch
import dask.dataframe as dd


class PerCategoryAnalysis(abc.ABC):
    @staticmethod
    @abc.abstractmethod
    def generate_data(rtd: dd.DataFrame) -> pd.DataFrame:
        pass

    @staticmethod
    @abc.abstractmethod
    def group_uncommon(data: pd.DataFrame) -> pd.DataFrame:
        pass

    @property
    @abc.abstractmethod
    def category_name(self) -> str:
        pass

    def plot_type_delay(self):
        delays = ((self.data['ar_delay_mean'] + self.data['dp_delay_mean']) / 2).to_numpy().astype(float)
        use_trains = np.logical_not(np.isnan(delays))
        delays = delays[use_trains]

        fig, ax = plt.subplots(subplot_kw=dict(aspect="equal"))
        ax.set_title(f'Verspätung pro {self.category_name}')
        ax.axis("off")

        type_count = (self.data.loc[use_trains, 'ar_delay_count'] + self.data.loc[use_trains, 'dp_delay_count']).to_numpy()

        cmap = matplotlib.colors.LinearSegmentedColormap.from_list(
            "", ["green", "yellow", "red"]
        )
        bubble_plot = BubbleChart(area=type_count, bubble_spacing=40)
        bubble_plot.collapse(n_iterations=100)
        bubble_plot.plot(
            ax,
            labels=self.data.loc[use_trains, :].index,
            colors=[cmap(delay) for delay in (delays - delays.min()) / max(delays - delays.min())]
        )

        # scatter in order to set colorbar
        scatter = ax.scatter(np.zeros(len(delays)), np.zeros(len(delays)), s=0, c=delays, cmap=cmap)
        colorbar = fig.colorbar(scatter)
        colorbar.solids.set_edgecolor("face")
        colorbar.outline.set_linewidth(0)
        colorbar.ax.get_yaxis().labelpad = 15
        colorbar.ax.set_ylabel("Ø Verspätung in Minuten", rotation=270)

        ax.relim()
        ax.autoscale_view()
        plt.show()

    def plot_type_cancellations(self):
        happened = ((self.data['ar_happened_mean'] + self.data['dp_happened_mean']) / 2).to_numpy().astype(float)
        use_trains = np.logical_not(np.isnan(happened))
        happened = happened[use_trains]

        fig, ax = plt.subplots(subplot_kw=dict(aspect="equal"))
        ax.set_title(f'Ausfälle pro {self.category_name}')
        ax.axis("off")
        type_count = (self.data.loc[use_trains, 'ar_delay_count'] + self.data.loc[use_trains, 'dp_delay_count']).to_numpy()

        cmap = matplotlib.colors.LinearSegmentedColormap.from_list(
            "", ["red", "yellow", "green"]
        )
        bubble_plot = BubbleChart(area=type_count, bubble_spacing=40)
        bubble_plot.collapse(n_iterations=100)

        bubble_plot.plot(
            ax,
            labels=self.data.loc[use_trains, :].index,
            colors=[cmap(delay) for delay in (happened - happened.min()) / max(happened - happened.min())]
        )

        # scatter in order to set colorbar
        scatter = ax.scatter(np.zeros(len(happened)), np.zeros(len(happened)), s=0, c=happened, cmap=cmap)
        colorbar = fig.colorbar(scatter)
        colorbar.solids.set_edgecolor("face")
        colorbar.outline.set_linewidth(0)
        colorbar.ax.get_yaxis().labelpad = 15
        colorbar.ax.set_ylabel("Anteil der tatsächlich stattgefundenen Halte", rotation=270)

        ax.relim()
        ax.autoscale_view()
        plt.show()


class TrainTypeAnalysis(PerCategoryAnalysis):

    category_name = 'Zugtyp'

    def __init__(self, rtd, **kwargs):
        self.data = cached_table_fetch(
            'per_train_type',
            index_col='c',
            table_generator=lambda: self.generate_data(rtd),
            push=True,
            **kwargs
        )

        self.data = self.group_uncommon(self.data)

    @staticmethod
    def generate_data(rtd: dd.DataFrame) -> pd.DataFrame:
        data = rtd.groupby('c', sort=False).agg({
            'ar_delay': ['count', 'mean'],
            'ar_happened': ['mean'],
            'dp_delay': ['count', 'mean'],
            'dp_happened': ['mean'],
        }).compute()

        data = groupby_index_to_flat(data)

        return data

    @staticmethod
    def group_uncommon(data: pd.DataFrame) -> pd.DataFrame:
        # group train types that are uncommon under 'other'
        count = data[('ar_delay_count')] + data[('dp_delay_count')]
        cutoff = count.nsmallest(50).max()
        combine_mask = count <= cutoff
        groups_to_combine = data.loc[combine_mask, :]
        other = data.iloc[0, :].copy()
        for col in groups_to_combine.columns:
            if 'count' in col:
                count_col = col
                count = groups_to_combine[col].sum()
                other.loc[col] = count
            else:
                other.loc[col] = (groups_to_combine[col] * groups_to_combine[count_col]).sum() / count
        data = data.loc[~combine_mask, :].copy()
        data.loc['other', :] = other

        return data


class OperatorAnalysis(PerCategoryAnalysis):

    category_name = 'Beteiber'

    def __init__(self, rtd, **kwargs):
        self.data = cached_table_fetch(
            'per_operator',
            index_col='o',
            table_generator=lambda: self.generate_data(rtd),
            push=True,
            **kwargs
        )

        self.data = self.group_uncommon(self.data)

    @staticmethod
    def generate_data(rtd: dd.DataFrame) -> pd.DataFrame:
        data = rtd.groupby('o', sort=False).agg({
            'ar_delay': ['count', 'mean'],
            'ar_happened': ['mean'],
            'dp_delay': ['count', 'mean'],
            'dp_happened': ['mean'],
        }).compute()

        data = groupby_index_to_flat(data)

        return data

    @staticmethod
    def group_uncommon(data: pd.DataFrame) -> pd.DataFrame:
        # group train types that are uncommon under 'other'
        count = data[('ar_delay_count')] + data[('dp_delay_count')]
        cutoff = count.nsmallest(200).max()
        combine_mask = count <= cutoff
        groups_to_combine = data.loc[combine_mask, :]
        other = data.iloc[0, :].copy()
        for col in groups_to_combine.columns:
            if 'count' in col:
                count_col = col
                count = groups_to_combine[col].sum()
                other.loc[col] = count
            else:
                other.loc[col] = (groups_to_combine[col] * groups_to_combine[count_col]).sum() / count
        data = data.loc[~combine_mask, :].copy()
        data.loc['other', :] = other

        return data


if __name__ == '__main__':
    import helpers.fancy_print_tcp
    rtd_ray = RtdRay()
    rtd = rtd_ray.load_data(columns=[
        'ar_pt',
        'dp_pt',
        'c',
        'o',
        'ar_delay',
        'ar_happened',
        'dp_delay',
        'dp_happened'
    ])
    rtd['c'] = rtd['c'].astype(str)
    rtd['o'] = rtd['o'].astype(str)

    tta = TrainTypeAnalysis(rtd=rtd, generate=False, prefer_cahce=True)
    tta.plot_type_delay()
    tta.plot_type_cancellations()

    oa = OperatorAnalysis(rtd=rtd, generate=False, prefer_cahce=True)
    oa.plot_type_delay()
    oa.plot_type_cancellations()
