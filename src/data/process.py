from __future__ import annotations

import logging

import pandas as pd

from utils.execution import TaskExecutor


class InitialProcessor:
    """Process extreme outliers, datetimes, timezone and sorting"""

    def __init__(self):
        pass

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        logging.debug(f'Preprocess shape: {df.shape}')
        steps = [
            self.remove_outliers,
            self.convert_dt,
            self.handle_timezone,
            self.sort_by_dt,
            self.remove_duplicates
        ]
        for step in steps:
            df = TaskExecutor.run_child_step(step, df)
        logging.debug(f'PostProcess shape: {df.shape}')
        return df

    def remove_outliers(self, df):
        df_ex1 = self.retrieve_extremes(df, 'count', .1, .9, 'High')
        df_ex2 = self.retrieve_extremes(df, 'price', .25, .75, 'Low')
        for df_ex in [df_ex1, df_ex2]:
            df = self.remove_extremes(df, df_ex)
        return df.dropna()

    @staticmethod
    def retrieve_extremes(df, col, low_q, high_q, extreme=None):
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

    @staticmethod
    def remove_extremes(df, df_extremes):
        """Remove rows contain UIDs from extremes"""
        return df[~df['uid'].isin(df_extremes['uid'])]

    @staticmethod
    def convert_dt(df):
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df

    @staticmethod
    def handle_timezone(df):
        df['time_zone'] = df['timestamp'].dt.tz
        df['time_zone'] = df['time_zone'].astype(str)
        df['timestamp'] = df['timestamp'].dt.tz_convert(None)
        return df

    @staticmethod
    def sort_by_dt(df):
        return df.sort_values(by='timestamp')

    @staticmethod
    def remove_duplicates(df):
        return df.drop_duplicates()
