from __future__ import annotations

import logging
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from pprint import pformat

from utils import my_globs


@dataclass
class DataState:
    raw_path: Path = Path(my_globs.project_config['raw']+'uber.csv')
    sdo_path: Path = Path(my_globs.project_config['sdo']+'uber.parquet')
    initial_process_path: Path = Path(my_globs.project_config['sdo']+'initial_process.parquet')
    features_path: Path = Path(my_globs.project_config['sdo']+'features.parquet')
    interim_data_path: Path = Path(my_globs.project_config['interim'])
    overwrite: bool = True

    def __post_init__(self):
        post_init_dict = {
            'raw_path': self.raw_path,
            'sdo_path': self.sdo_path,
            'initial_process_path': self.initial_process_path
        }
        logging.debug(f"Initialized DataState:\n{pformat(post_init_dict)}")

    def get_paths(self):
        return {
            'raw': self.raw_path,
            'sdo': self.sdo_path,
            'initial_process': self.initial_process_path,
            'features': self.features_path,
            'interim_data': self.interim_data_path
        }

    def __repr__(self):
        return pformat(self.__dict__)


@dataclass
class DataConfig:
    # sma_windows: list = field(default_factory=lambda: ['30T', '1H', '2H', '4H'])
    sma_windows: list = field(default_factory=lambda: [1, 2, 4, 6])
