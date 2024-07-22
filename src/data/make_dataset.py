from __future__ import annotations

import logging

from config import DataState
from utils.file_access import FileAccess


class MakeDataset:
    def __init__(self, data_state: DataState):
        self.ds = data_state

    def base_process(self, df):
        rename_map = {
            'Unnamed: 0': 'uid',
            'pickup_datetime': 'timestamp',
            'fare_amount': 'price',
            'passenger_count': 'count'}
        df = df.rename(columns=rename_map)
        return df.drop(columns=['key'])

    def pipeline(self, load_path, save_path):
        logging.debug('Starting MakeDataset')

        try:
            df = FileAccess.load_file(load_path)
            df = self.base_process(df)
            FileAccess.save_file(df, save_path, self.ds.overwrite)
        except Exception as e:
            logging.exception(f'Error: {e}', exc_info=e)
            return None

        logging.debug('Completed MakeDataset')
        return df
