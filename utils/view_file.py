from __future__ import annotations

import logging
from pathlib import Path

from utils.file_access import FileAccess


def view_file(filepath):
    logging.info(FileAccess.load_file(filepath))


def view_dir_data(directory: Path, suffix: str):
    file_store = [file for file in directory.rglob(f'*{suffix}') if file.is_file()]
    for file in sorted(file_store):
        x = FileAccess.load_file(file)
        logging.info(x)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)

    directory = Path('data/interim')
    suffix = '.parquet'
    # view_dir_data(directory, suffix)

    view_file('data/sdo/uber.parquet')
