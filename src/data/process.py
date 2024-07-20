from __future__ import annotations

import logging

from config import DataState
from utils.file_access import FileAccess


class InitialProcessor:
    def __init__(self, data_state: DataState):
        self.ds = data_state
        self.file_access = FileAccess()
        self.load_path = self.ds.sdo_path
        self.save_path = self.ds.initial_process_path

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

    def pipeline(self):
        df = self.file_access.read_file(self.load_path)
        logging.debug(
            f'Starting InitialProcessor. Preprocess shape: {df.shape}')

        df_ex1 = self.retrieve_extremes(df, 'passenger_count', .1, .9, 'High')
        df_ex2 = self.retrieve_extremes(df, 'fare_amount', .25, .75, 'Low')
        for df_ex in [df_ex1, df_ex2]:
            df = self.remove_extremes(df, df_ex)
        df = df.dropna()
        df = df.drop_duplicates()
        self.file_access.save_file(df, self.save_path)

        logging.debug(
            f'Completed InitialProcessor. Preprocess shape: {df.shape}')
        return df


class FurtherProcessor:
    def __init__(self, data_state: DataState):
        self.ds = data_state
        self.file_access = FileAccess()
        self.load_path = self.ds.sdo_path

    def load_data(self):
        return self.file_access.read_file(self.load_path)
