from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np

from config import DataConfig
from config import DataState


class Visualiser:
    def __init__(self, data_state: DataState, data_config: DataConfig):
        self.ds = data_state
        self.dc = data_config
        self.load_path = self.ds.initial_process_path

    def get_patterns(self, df, pattern):
        return [col for col in df.columns if pattern in col]

    def generate_sma_plots(self, df, column):
        # cols = self.get_patterns(df, 'sma')
        start_index = np.random.randint(0, len(df))
        sample = df.iloc[start_index:start_index + 1000]
        for hour in self.dc.sma_windows:
            plt.plot(sample.index, sample[f'{column}_sma_{hour}'], label=hour, linewidth=0.5)
        plt.legend()
        plt.show()
