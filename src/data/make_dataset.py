from __future__ import annotations

import logging

from config import DataState
from utils.file_access import FileAccess


class MakeDataset:
    def __init__(self, data_state: DataState):
        self.ds = data_state
        self.load_path = self.ds.raw_path
        self.save_path = self.ds.sdo_path

    def load_data(self):
        return FileAccess.read_file(self.load_path)

    def base_process(self, df):
        rename_map = {'Unnamed: 0': 'uid'}
        df = df.rename(columns=rename_map)
        return df.drop(columns=['key'])

    def convert_data(self, df):
        df.to_parquet(self.save_path)
        return df

    def pipeline(self):
        logging.debug('Starting MakeDataset')

        try:
            if not self.save_path.exists():
                df = self.load_data()
                df = self.base_process(df)
                df = self.convert_data(df)
            else:
                print(self.save_path)
                df = FileAccess.read_file(self.save_path)
                return df
        except Exception as e:
            logging.exception(f'Error: {e}', exc_info=e)
            return None

        logging.debug('Completed MakeDataset')
        return df
