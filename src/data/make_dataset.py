from __future__ import annotations

import pandas as pd

from config.data import DataConfig


class MakeDataset:
    """Load dataset and perform base processing"""

    def __init__(self, data_config: DataConfig):
        self.dc = data_config

    def base_process(self, df: pd.DataFrame) -> pd.DataFrame:
        rename_map = {
            'Unnamed: 0': 'uid',
            'pickup_datetime': 'timestamp',
            'fare_amount': 'price',
            'passenger_count': 'count'}
        df = df.rename(columns=rename_map)
        return df.drop(columns=['key'])

    def pipeline(self, df):
        df = self.base_process(df)
        return df
