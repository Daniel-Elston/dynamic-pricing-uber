from __future__ import annotations

from config.state_init import StateManager
from src.data.make_dataset import MakeDataset
from src.data.process import InitialProcessor
from src.features.bound_analysis import AnalyseBounds
from src.features.build_features import BuildAnalysisFeatures
from src.features.build_model_features import BuildModelFeatures
from src.models.pricing import DynamicPricing
from utils.execution import TaskExecutor


class DataPipeline:
    def __init__(self, state: StateManager, exe: TaskExecutor):
        self.state = state
        self.exe = exe

    def main(self):
        steps = [
            (MakeDataset(), 'raw', 'sdo'),
            (InitialProcessor(), 'sdo', 'process1'),
            (BuildAnalysisFeatures(self.state), 'process1', 'features1'),
            (AnalyseBounds(self.state), 'features1', None),
            (BuildModelFeatures(), 'process1', 'features2'),
            (DynamicPricing(self.state), 'features2', 'result'),
        ]
        for step, load_path, save_paths in steps:
            self.exe.run_parent_step(step.pipeline, load_path, save_paths)
