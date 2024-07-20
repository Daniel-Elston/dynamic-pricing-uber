from __future__ import annotations

import logging
from dataclasses import dataclass
from dataclasses import field
from pprint import pformat

import pandas as pd


@dataclass
class DataState:
    df: pd.DataFrame = None
    checkpoints: list = field(default_factory=lambda: ['some', 'checkpoint', 'items'])

    def __post_init__(self):
        post_init_dict = {
            'df': self.df,
        }
        logging.debug(f"Initialized DataState: {pformat(post_init_dict)}")

    def __repr__(self):
        return pformat(self.__dict__)
