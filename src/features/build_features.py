from __future__ import annotations

import logging

from config import DataConfig
from config import DataState
from utils.file_access import FileAccess


class BuildFeatures:
    def __init__(self, data_state: DataState, data_config: DataConfig):
        self.ds = data_state
        self.dc = data_config
        self.load_path = self.ds.initial_process_path
        self.save_path = self.ds.features_path

    def build_dt_features(self, df):
        df['dow'] = df['timestamp'].dt.day_name().str[:3]
        df['hour'] = df['timestamp'].dt.hour
        df['date'] = df['timestamp'].dt.date
        df['week'] = df['timestamp'].dt.isocalendar().week
        df['month'] = df['timestamp'].dt.month
        df['date_hour'] = df['date'].astype(str)+'-'+df['hour'].astype(str)
        return df

    def pipeline(self):
        logging.debug(
            'Starting Feature Building Pipeline')

        try:
            df = FileAccess.load_file(self.load_path)
            df = self.build_dt_features(df)
            FileAccess.save_file(df, self.save_path, self.ds.overwrite)

        except Exception as e:
            logging.exception(f'Error: {e}', exc_info=True)
            raise
        logging.debug(
            'Completed Feature Building Pipeline')
