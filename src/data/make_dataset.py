from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

import utils.my_globs as my_globs
from utils.file_handler import FileHandler


class MakeDataset:
    def __init__(self):
        self.fh = FileHandler()
        self.load_path = Path(my_globs.project_config['path']['raw'])
        self.save_path = Path(my_globs.project_config['path']['sdo'])

    def load_data(self):
        return pd.read_excel(self.load_path/'data.xlsx')

    def rename_cols(self, df):
        rename_map = {
            'raw-col-title': 'new-col-title',
        }
        return df.rename(columns=rename_map)

    def pipeline(self):
        logging.info('Starting Data Pipeline')
        df = self.load_data()
        df = self.rename_cols(df)
        return df
