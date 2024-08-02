from __future__ import annotations

import pandas as pd

from config.data import DataConfig
from config.data import DataState


class BuildFeatures:
    """Build/compose simple datetime features"""

    def __init__(self, data_state: DataState, data_config: DataConfig):
        self.ds = data_state
        self.dc = data_config

    def build_dt_features(self, df):
        df['dow'] = df['timestamp'].dt.day_name().str[:3]
        df['hour'] = df['timestamp'].dt.hour.astype('int32')
        df['date'] = df['timestamp'].dt.date.astype('datetime64[ns]')
        df['week'] = df['timestamp'].dt.isocalendar().week.astype('UInt32')
        df['month'] = df['timestamp'].dt.month.astype('int32')
        # df['date_hour'] = df['date'].astype(str)+'-'+df['hour'].astype(str)
        df['date_hour'] = df['timestamp'].dt.strftime('%Y-%m-%d-%H')

        return df

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self.build_dt_features(df)
        return df
