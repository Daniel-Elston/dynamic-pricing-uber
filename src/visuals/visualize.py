from __future__ import annotations

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

from config.data import DataConfig
from config.data import DataState
from utils.file_access import FileAccess


class Visualiser:
    def __init__(self, data_state: DataState, data_config: DataConfig):
        self.ds = data_state
        self.dc = data_config

    def _get_patterns(self, df, pattern):
        return [col for col in df.columns if pattern in col]

    def generate_sma_plots(self, df, column):
        # cols = self._get_patterns(df, 'sma')
        start_index = np.random.randint(0, len(df))
        sample = df.iloc[start_index:start_index + 1000]
        for hour in self.dc.sma_windows:
            plt.plot(sample.index, sample[f'{column}_sma_{hour}'], label=hour, linewidth=0.5)
        plt.legend()
        plt.show()


class BoundVisuals:
    @staticmethod
    def bound_hours(load_path):
        directory = Path(load_path).rglob('*.parquet')
        for file in sorted(list(directory))[1:5]:
            dataset = FileAccess.load_file(file)
            filename = file.stem

            plt.figure(figsize=(18, 9))
            sns.histplot(dataset[['hour']])
            plt.savefig(f'reports/figures/{filename}.png')
            plt.show()
            logging.debug(f'Count plot for {filename} saved to ``reports/figures/{filename}.png``')
