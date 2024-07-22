from __future__ import annotations

import logging

import pandas as pd

from config import DataState
from utils.file_access import FileAccess


class InitialProcessor:
    def __init__(self, data_state: DataState):
        self.ds = data_state

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

    def pipeline(self, load_path, save_path):
        df = FileAccess.load_file(load_path)
        logging.debug(
            f'Starting InitialProcessor. Preprocess shape: {df.shape}')

        try:
            df_ex1 = self.retrieve_extremes(df, 'count', .1, .9, 'High')
            df_ex2 = self.retrieve_extremes(df, 'price', .25, .75, 'Low')
            for df_ex in [df_ex1, df_ex2]:
                df = self.remove_extremes(df, df_ex)
            df = df.dropna()

            df = self.convert_dt(df)
            df = self.handle_timezone(df)
            df = self.sort_by_dt(df)
            df = df.drop_duplicates()
            FileAccess.save_file(df, save_path, self.ds.overwrite)
        except Exception as e:
            logging.exception(f'Error: {e}', exc_info=e)
            raise

        logging.debug(
            f'Completed InitialProcessor. PostProcess shape: {df.shape}')
        return df


# class FurtherProcessor:
#     def __init__(self, data_state: DataState):
#         self.ds = data_state

#     def load_data(self):
#         return self.file_access.read_file(load_path)
