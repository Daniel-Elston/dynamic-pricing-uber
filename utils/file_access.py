from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from pathlib import Path

import pandas as pd


class FileAccess:
    @staticmethod
    def extract_suffix(path: Path):
        return path.suffix

    @staticmethod
    @contextmanager
    def load_file(path: Path):
        path = Path(path)
        suffix = FileAccess.extract_suffix(path)
        logging.debug(f'Reading file: ``{path}``')
        if suffix == '.parquet':
            df = pd.read_parquet(path)
        elif suffix == '.csv':
            df = pd.read_csv(path)
        elif suffix == '.xlsx':
            df = pd.read_excel(path)
        elif suffix == '.json':
            df = pd.read_json(path)
        else:
            raise ValueError(f'Unknown file type: {suffix}')
        yield df

    @staticmethod
    def save_helper(df: pd.DataFrame, path: Path):
        suffix = FileAccess.extract_suffix(path)
        if suffix == '.parquet':
            return df.to_parquet(path, index=False)
        elif suffix == '.csv':
            return df.to_csv(path, index=False)
        elif suffix == '.xlsx':
            return df.to_excel(path, index=False)
        elif suffix == '.json':
            return df.to_json(path)
        else:
            raise ValueError(f'Unknown file type: {path} {suffix}')

    @staticmethod
    @contextmanager
    def save_file(df: pd.DataFrame, path: Path, overwrite=False):
        path = Path(path)
        if overwrite is False and path.exists():
            logging.warning(f'File already exists: ``{path}``')
        else:
            logging.debug(f'Saving file: ``{path}``')
            FileAccess.save_helper(df, path)

    @staticmethod
    @contextmanager
    def save_json(data, path, overwrite=False):
        if overwrite is False and Path(path).exists():
            logging.warning(f'File already exists: ``{path}``')
        else:
            logging.debug(f'Saving json to ``{path}``')
            with open(path, 'w') as file:
                json.dump(data, file)

    @staticmethod
    @contextmanager
    def load_json(path):
        with open(path, 'r') as file:
            return json.load(file)

    @staticmethod
    def form_path(dir_path, filename):
        return Path.joinpath(dir_path, filename)


if __name__ == "__main__":
    file_path = Path('data/sdo/uber.parquet')
    df = FileAccess.load_file(file_path)
    print(df.head(20))
