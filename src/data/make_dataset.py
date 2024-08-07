from __future__ import annotations

import warnings

import pandas as pd

from config.data import DataConfig
from config.data import DataState
from utils.running import Running
warnings.filterwarnings("ignore")


class MakeDataset:
    """Load dataset and perform base processing"""

    def __init__(self, data_state: DataState, data_config: DataConfig):
        self.ds = data_state
        self.dc = data_config
        self.runner = Running(self.ds, self.dc)

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        steps = [
            self.base_process,
        ]
        for step in steps:
            df = self.runner.run_child_step(step, df)
        return df

    def base_process(self, df):
        rename_map = {
            'Unnamed: 0': 'uid',
            'pickup_datetime': 'timestamp',
            'fare_amount': 'price',
            'passenger_count': 'count'}
        df = df.rename(columns=rename_map)
        return df.drop(columns=['key'])
