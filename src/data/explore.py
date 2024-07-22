from __future__ import annotations

import logging
from pathlib import Path

from config import DataState
from utils.file_access import FileAccess


class InitialExploration:
    def __init__(self, data_state: DataState):
        self.ds = data_state
        self.load_path = self.ds.sdo_path

    def generate_metadata(self, df):
        logging.debug(df)
        logging.debug(df.info(memory_usage='deep'))  # Covert pickup_datetime
        logging.debug(df.describe())  # Visualise max/min fares for errors
        logging.debug(df.isna().sum())  # Drop NaNs

        meta_report_path = Path('reports/analysis/metadata.xlsx')
        if not meta_report_path.exists():
            df.describe().to_excel(meta_report_path)

    def pipeline(self):
        logging.debug(
            'Starting MakeDataset')

        df = FileAccess.load_file(self.load_path)
        self.generate_metadata(df)

        logging.debug(
            'Completed MakeDataset')
        return df
