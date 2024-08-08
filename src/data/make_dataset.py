from __future__ import annotations

import warnings

import pandas as pd

from utils.execution import TaskExecutor
warnings.filterwarnings("ignore")


class MakeDataset:
    """Load dataset and perform base processing"""

    def __init__(self):
        pass

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        steps = [
            self.base_process,
        ]
        for step in steps:
            df = TaskExecutor.run_child_step(step, df)
        return df

    def base_process(self, df):
        rename_map = {
            'Unnamed: 0': 'uid',
            'pickup_datetime': 'timestamp',
            'fare_amount': 'price',
            'passenger_count': 'count'}
        df = df.rename(columns=rename_map)
        return df.drop(columns=['key'])
