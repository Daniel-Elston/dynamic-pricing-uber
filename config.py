from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from pprint import pformat

from utils import my_globs
# from dataclasses import field


@dataclass
class DataState:
    raw_path: Path = Path(my_globs.project_config['raw']+'uber.csv')
    sdo_path: Path = Path(my_globs.project_config['sdo']+'uber.parquet')
    initial_process_path: Path = Path(my_globs.project_config['sdo']+'initial_process.parquet')

    def __post_init__(self):
        post_init_dict = {
            'raw_path': self.raw_path,
            'sdo_path': self.sdo_path,
            'initial_process_path': self.initial_process_path
        }
        logging.debug(f"Initialized DataState:\n{pformat(post_init_dict)}")

    def __repr__(self):
        return pformat(self.__dict__)
