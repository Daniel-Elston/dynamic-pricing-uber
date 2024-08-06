from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from utils.file_access import FileAccess


def view_file(filepath):
    x = pd.read_parquet(filepath)
    logging.info(x)
    return x


def view_dir_data(directory: Path, suffix: str):
    file_store = [file for file in directory.rglob(f'*{suffix}') if file.is_file()]
    for file in sorted(file_store):
        x = FileAccess.load_file(file)
        logging.info(x)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)

    directory = Path('data/result_frames/')
    suffix = '.parquet'
    # view_dir_data(directory, suffix)

    # view_file('data/sdo/initial_features.parquet')
    x = view_file('data/result_frames/bounds.parquet')
    x.head(100).to_csv('bounds.csv')
    # view_file('data/interim/min_ppm_hour.parquet')
