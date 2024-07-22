from __future__ import annotations

import logging

from config import DataState
from utils.file_access import FileAccess


class MakeDataset:
    def __init__(self, data_state: DataState):
        self.ds = data_state
        self.load_path = self.ds.raw_path
        self.save_path = self.ds.sdo_path

    def base_process(self, df):
        rename_map = {
            'Unnamed: 0': 'uid',
            'pickup_datetime': 'timestamp',
            'fare_amount': 'price',
            'passenger_count': 'count'}
        df = df.rename(columns=rename_map)
        return df.drop(columns=['key'])

    def pipeline(self):
        logging.debug('Starting MakeDataset')

        try:
            df = FileAccess.load_file(self.load_path)
            df = self.base_process(df)
            FileAccess.save_file(df, self.save_path, self.ds.overwrite)
        except Exception as e:
            logging.exception(f'Error: {e}', exc_info=e)
            return None

        logging.debug('Completed MakeDataset')
        return df
