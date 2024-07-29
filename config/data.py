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
    initial_features_path: Path = Path('data/sdo/initial_features.parquet')
    bound_features_path: Path = Path('data/sdo/bound_features.parquet')

    interim_data_path: Path = Path('data/interim/')

    frame_result_path: Path = Path('data/result/')
    result_path: Path = Path('reports/results/bound_hours.json')

    def get_paths(self):
        return {
            'raw': self.raw_path,
            'sdo': self.sdo_path,
            'process1': self.initial_process_path,
            'features1': self.initial_features_path,
            'features2': self.bound_features_path,
            'interim': self.interim_data_path,

            'frame_result': self.frame_result_path,
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
    window_select: int = 4
    sma_windows: list = field(default_factory=lambda: [1, 2, 4, 6])
