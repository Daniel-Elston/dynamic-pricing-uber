from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from sklearn.preprocessing import MinMaxScaler

from config.data import DataConfig
from config.data import DataState
from config.model import ModelConfig
sns.set_theme(style="whitegrid")


class AnalyseBounds:
    def __init__(self, data_state: DataState, data_config: DataConfig, model_config: ModelConfig):
        self.ds = data_state
        self.dc = data_config
        self.mc = model_config
        self.scaler = MinMaxScaler()

    def analyze_ratio_distribution(self, df, ratio_column, time_periods, bins):
        df['time_period'] = pd.cut(
            df['timestamp'].dt.hour,
            bins=[period[0] for period in time_periods.values()] + [24],
            labels=time_periods.keys(),
            include_lowest=True
        )

        thresholds = self._tuple_to_bin_edges(bins)
        bin_edges = sorted(list(thresholds.values()))
        bin_labels = [f'{bin_edges[i]:.2f}-{bin_edges[i+1]:.2f}' for i in range(len(bin_edges)-1)]

        df['ratio_bin'] = pd.cut(df[ratio_column], bins=bin_edges, labels=bin_labels, include_lowest=True)
        ratio_counts = df.groupby(['time_period', 'ratio_bin']).size().unstack(fill_value=0)

        return ratio_counts

    def plot_ratio_distribution(self, ratio_counts, ratio_column, window_size):
        fig, ax = plt.subplots(figsize=(20, 10))

        x = np.arange(len(ratio_counts.index))
        width = 0.15
        multiplier = 0

        for column in ratio_counts.columns:
            offset = width * multiplier
            rects = ax.bar(x + offset, ratio_counts[column], width, label=column)
            ax.bar_label(rects, padding=3, rotation=90, fontsize=10)
            multiplier += 1

        ax.set_ylabel('Count', fontsize=12)
        ax.set_xlabel('Time Period', fontsize=12)
        ax.set_title(f'Distribution of {ratio_column} ({window_size}-hour windows)', fontsize=14)
        ax.set_xticks(x + width * (len(ratio_counts.columns) - 1) / 2)
        ax.set_xticklabels(ratio_counts.index, rotation=45, ha='right', fontsize=10)
        ax.legend(loc='upper left', ncols=1, fontsize=10)
        ax.set_ylim(0, ratio_counts.values.max() * 1.15)
        plt.tight_layout()
        plt.savefig(f'reports/figures/bound_analysis/{ratio_column}.png')
        plt.show()

    def analyze_and_plot_ratio_distributions(self, df, time_periods, window_size, bins, ratio_columns):
        for ratio_column in ratio_columns:
            ratio_counts = self.analyze_ratio_distribution(df, ratio_column, time_periods, bins)
            self.plot_ratio_distribution(ratio_counts, ratio_column, window_size)
        return ratio_counts

    def _tuple_to_bin_edges(self, bins):
        return {k[0]: k[1] for k in bins.keys()}

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.dropna()

        self.analyze_and_plot_ratio_distributions(
            df, self.mc.time_periods_3hr, 3, self.mc.max_ratio_bins, [
                '3h_partly_cpm_max_ratio', '3h_partly_cpm_max_ratio_scaled'])

        self.analyze_and_plot_ratio_distributions(
            df, self.mc.time_periods_3hr, 3, self.mc.mean_ratio_bins, [
                '3h_partly_cpm_mean_ratio_scaled'])

        return df
