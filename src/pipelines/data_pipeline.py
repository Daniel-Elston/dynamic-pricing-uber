from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from config.data import DataConfig
from config.data import DataState
from config.model import ModelConfig
from src.data.make_dataset import MakeDataset
from src.data.process import InitialProcessor
from src.features.bound_analysis import AnalyseBounds
from src.features.build_features import BuildFeatures
from src.features.build_model_features import BuildModelFeatures
from src.models.pricing import DynamicPricing
from utils.running import Running
# from src.features.build_ped import BuildPED
# from src.models.train import TrainModel


class DataPipeline:
    def __init__(self, data_state: DataState, data_config: DataConfig, model_config: ModelConfig):
        self.ds = data_state
        self.dc = data_config
        self.mc = model_config
        self.runner = Running(self.ds, self.dc)

    def main(self):
        steps = [
            (self.run_make_dataset, 'raw', 'sdo'),
            (self.run_initial_processor, 'sdo', 'process1'),
            (self.run_build_features, 'process1', 'features1'),
            (self.run_bounds_analysis, 'features1', None),
            (self.run_build_model_features, 'process1', 'features2'),
            (self.run_dynamic_pricing, 'features2', None),
        ]
        [self.runner.run_step(step, load_path, save_paths) for step, load_path, save_paths in steps]
        logging.info(f"{self.__class__.__name__} completed SUCCESSFULLY")

    def run_make_dataset(self, df: pd.DataFrame, save_path: Path) -> pd.DataFrame:
        return MakeDataset(self.dc).pipeline(df)

    def run_initial_processor(self, df: pd.DataFrame, save_path: Path) -> pd.DataFrame:
        return InitialProcessor(self.dc).pipeline(df)

    def run_build_features(self, df: pd.DataFrame, save_path: Path) -> pd.DataFrame:
        return BuildFeatures(self.ds, self.dc).pipeline(df)

    def run_bounds_analysis(self, df: pd.DataFrame, save_path: Path) -> pd.DataFrame:
        return AnalyseBounds(self.ds, self.dc, self.mc).pipeline(df)

    def run_build_model_features(self, df: pd.DataFrame, save_path: Path) -> pd.DataFrame:
        """Build Minimal Features required for Alg."""
        return BuildModelFeatures(self.ds, self.dc).pipeline(df)

    def run_dynamic_pricing(self, df: pd.DataFrame, save_path: Path) -> pd.DataFrame:
        return DynamicPricing(self.mc).pipeline(df)

    # def run_build_ped(self, df: pd.DataFrame, save_path: Path) -> pd.DataFrame:
    #     return BuildPED(self.ds, self.dc).pipeline(df)

    # def run_train_model(self, df: pd.DataFrame, save_path: Path) -> pd.DataFrame:
    #     return TrainModel().pipeline(df)
