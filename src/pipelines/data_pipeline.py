from __future__ import annotations

import logging
from pathlib import Path
from typing import Callable
from typing import List
from typing import Union

import pandas as pd

from config.data import DataConfig
from config.data import DataState
from src.data.make_dataset import MakeDataset
from src.data.process import InitialProcessor
from src.features.build_datasets import BuildBounds
from src.features.build_datasets import BuildMovingAverages
from src.features.build_features import BuildFeatures
from src.features.build_ped import BuildPED
from src.visuals.visualize import BoundVisuals
from utils.file_access import FileAccess
from utils.logging_utils import log_step


class DataPipeline:
    def __init__(self, data_state: DataState, data_config: DataConfig):
        self.ds = data_state
        self.dc = data_config

    def run_step(self, step: Callable, load_path: str, save_paths: Union[str, List[str], None] = None):
        load_path = self.ds.paths.get_path(load_path)

        if save_paths is not None:
            save_paths = self.ds.paths.get_path(save_paths) if isinstance(save_paths, str) else [self.ds.paths.get_path(path) for path in save_paths]

        with FileAccess.load_file(load_path) as df:
            logged_step = log_step(load_path, save_paths)(step)
            result = logged_step(df, save_paths if isinstance(save_paths, str) else save_paths)

            if isinstance(save_paths, str):
                FileAccess.save_file(result, save_paths, self.dc.overwrite)

        return result

    def main(self):
        steps = [
            (self.run_make_dataset, 'raw', 'sdo'),
            (self.run_initial_processor, 'sdo', 'process1'),
            (self.run_build_features, 'process1', 'features1'),
            (self.run_build_moving_averages, 'features1', 'frame_result1'),
            (self.run_build_bounds, 'features1', ['interim', 'result1', 'frame_result2']),
            (self.run_build_ped, 'frame_result1', None),
        ]
        [self.run_step(step, load_path, save_paths) for step, load_path, save_paths in steps]
        logging.info(f"{self.__class__.__name__} completed SUCCESSFULLY")

    def run_make_dataset(self, df: pd.DataFrame, save_path: Path) -> pd.DataFrame:
        return MakeDataset(self.dc).pipeline(df)

    def run_initial_processor(self, df: pd.DataFrame, save_path: Path) -> pd.DataFrame:
        return InitialProcessor(self.dc).pipeline(df)

    def run_build_features(self, df: pd.DataFrame, save_path: Path) -> pd.DataFrame:
        return BuildFeatures(self.ds, self.dc).pipeline(df)

    def run_build_moving_averages(self, df: pd.DataFrame, save_path: Path) -> pd.DataFrame:
        return BuildMovingAverages(self.ds, self.dc).pipeline(df)

    def run_build_bounds(self, df: pd.DataFrame, save_path: List[Path]) -> pd.DataFrame:
        return BuildBounds(self.ds, self.dc).pipeline(df, save_path)

    def run_build_ped(self, df: pd.DataFrame, save_path: Path) -> pd.DataFrame:
        return BuildPED(self.ds, self.dc).pipeline(df)

    def run_bound_visuals(self, load_path):
        return BoundVisuals.bound_hours(load_path)
