from __future__ import annotations

import logging

import pandas as pd

from config.data import DataConfig


class InitialProcessor:
    """Process extreme outliers, datetimes, timezone and sorting"""

    def __init__(self, data_config: DataConfig):
        self.dc = data_config

    def retrieve_extremes(self, df, col, low_q, high_q, extreme=None):
        q1 = df[col].quantile(low_q)
        q3 = df[col].quantile(high_q)
        iqr = q3 - q1
        low = q1 - 1.5*iqr
        high = q3 + 1.5*iqr
        if extreme == 'High':
            df = df[(df[col] > high)]
        elif extreme == 'Low':
            df = df[(df[col] < low)]
        else:
            df = df[(df[col] < low) & (df[col] > high)]
        return df

    def remove_extremes(self, df, df_extremes):
        """Remove rows contain UIDs from extremes"""
        df = df[~df['uid'].isin(df_extremes['uid'])]
        return df

    def convert_dt(self, df):
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df

    def handle_timezone(self, df):
        df['time_zone'] = df['timestamp'].dt.tz
        df['time_zone'] = df['time_zone'].astype(str)
        df['timestamp'] = df['timestamp'].dt.tz_convert(None)
        return df

    def sort_by_dt(self, df):
        return df.sort_values(by='timestamp')

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        logging.debug(f'Preprocess shape: {df.shape}')

        df_ex1 = self.retrieve_extremes(df, 'count', .1, .9, 'High')
        df_ex2 = self.retrieve_extremes(df, 'price', .25, .75, 'Low')
        for df_ex in [df_ex1, df_ex2]:
            df = self.remove_extremes(df, df_ex)
        df = df.dropna()

        df = self.convert_dt(df)
        df = self.handle_timezone(df)
        df = self.sort_by_dt(df)
        df = df.drop_duplicates()

        logging.debug(f'PostProcess shape: {df.shape}')
        return df
