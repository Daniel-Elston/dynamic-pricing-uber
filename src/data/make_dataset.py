from __future__ import annotations

import logging

from config import DataState
from utils.file_access import FileAccess


class MakeDataset:
    def __init__(self, data_state: DataState):
        self.ds = data_state
        self.file_access = FileAccess()
        self.load_path = self.ds.raw_path
        self.save_path = self.ds.sdo_path

    def load_data(self):
        return self.file_access.read_file(self.load_path)

    def base_process(self, df):
        rename_map = {'Unnamed: 0': 'uid'}
        df = df.rename(columns=rename_map)
        return df.drop(columns=['key'])

    def convert_data(self, df):
        if not self.save_path.exists():
            df.to_parquet(self.save_path)
            return df
        else:
            df = self.file_access.read_file(self.save_path)
            return df

    def pipeline(self):
        logging.debug(
            'Starting MakeDataset')
        df = self.load_data()
        df = self.base_process(df)
        df = self.convert_data(df)
        logging.debug(
            'Completed MakeDataset')
        return df
