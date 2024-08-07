from __future__ import annotations

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


class DataPipeline:
    def __init__(self, data_state: DataState, data_config: DataConfig, model_config: ModelConfig):
        self.ds = data_state
        self.dc = data_config
        self.mc = model_config
        self.runner = Running(self.ds, self.dc)

    def main(self):
        steps = [
            (MakeDataset(self.ds, self.dc), 'raw', 'sdo'),
            (InitialProcessor(self.ds, self.dc), 'sdo', 'process1'),
            (BuildFeatures(self.ds, self.dc), 'process1', 'features1'),
            (AnalyseBounds(self.ds, self.dc, self.mc), 'features1', None),
            (BuildModelFeatures(self.ds, self.dc), 'process1', 'features2'),
            (DynamicPricing(self.ds, self.dc, self.mc), 'features2', 'result'),
        ]
        for step, load_path, save_paths in steps:
            self.runner.run_parent_step(step.pipeline, load_path, save_paths)
