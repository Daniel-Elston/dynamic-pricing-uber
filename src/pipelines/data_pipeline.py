from __future__ import annotations

from config.data import DataState
from config.model import ModelConfig
from src.data.make_dataset import MakeDataset
from src.data.process import InitialProcessor
from src.features.bound_analysis import AnalyseBounds
from src.features.build_features import BuildAnalysisFeatures
from src.features.build_model_features import BuildModelFeatures
from src.models.pricing import DynamicPricing
from utils.execution import TaskExecutor


class DataPipeline:
    def __init__(self, exe: TaskExecutor, data_state: DataState, model_config: ModelConfig):
        self.exe = exe
        self.ds = data_state
        self.dc = self.ds.config
        self.mc = model_config

    def main(self):
        steps = [
            (MakeDataset(), 'raw', 'sdo'),
            (InitialProcessor(), 'sdo', 'process1'),
            (BuildAnalysisFeatures(self.dc), 'process1', 'features1'),
            (AnalyseBounds(self.mc), 'features1', None),
            (BuildModelFeatures(), 'process1', 'features2'),
            (DynamicPricing(self.mc), 'features2', 'result'),
        ]
        for step, load_path, save_paths in steps:
            self.exe.run_parent_step(step.pipeline, load_path, save_paths)
