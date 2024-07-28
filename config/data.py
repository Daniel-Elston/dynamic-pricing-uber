from __future__ import annotations

import logging
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from pprint import pformat


@dataclass
class DataState:
    paths: dict = field(init=False)
    raw_path: Path = Path('data/raw/uber.csv')
    sdo_path: Path = Path('data/sdo/uber.parquet')
    initial_process_path: Path = Path('data/sdo/initial_process.parquet')
    features_path: Path = Path('data/sdo/features.parquet')
    interim_data_path: Path = Path('data/interim/')

    result_path: Path = Path('reports/results/bound_hours.json')

    def get_paths(self):
        return {
            'raw': self.raw_path,
            'sdo': self.sdo_path,
            'process1': self.initial_process_path,
            'features': self.features_path,
            'interim': self.interim_data_path,

            'result1': self.result_path
        }

    def __post_init__(self):
        self.paths = self.get_paths()
        post_init_dict = {
            'data_state_paths': self.paths
        }
        logging.debug(f"Initialized DataState:\n{pformat(post_init_dict)}")

    def __repr__(self):
        return pformat(self.__dict__)


@dataclass
class DataConfig:
    overwrite: bool = True
    save_fig: bool = True
    # sma_windows: list = field(default_factory=lambda: ['30T', '1H', '2H', '4H'])
    sma_windows: list = field(default_factory=lambda: [1, 2, 4, 6])
