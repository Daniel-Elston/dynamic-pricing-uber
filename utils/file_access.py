from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd


class FileAccess:
    def __init__(self):
        pass

    def extract_suffix(self, path: Path):
        return path.suffix

    def read_file(self, path: Path):
        suffix = self.extract_suffix(path)
        logging.debug(f'Reading file: {path}')
        if suffix == '.parquet':
            return pd.read_parquet(path)
        elif suffix == '.csv':
            return pd.read_csv(path)
        elif suffix == '.xlsx':
            return pd.read_excel(path)
        elif suffix == '.json':
            return pd.read_json(path)
        else:
            raise ValueError(f'Unknown file type: {suffix}')


if __name__ == "__main__":
    file_path = Path('path.parquet')
    df = FileAccess.read_file(file_path)
    print(df.head())
