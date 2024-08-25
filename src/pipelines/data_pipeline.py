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
            (MakeDataset().pipeline, 'raw', 'sdo'),
            (InitialProcessor().pipeline, 'sdo', 'process1'),
            (BuildAnalysisFeatures(self.state).pipeline, 'process1', 'features1'),
            (AnalyseBounds(self.state).pipeline, 'features1', None),
            (BuildModelFeatures().pipeline, 'process1', 'features2'),
            (DynamicPricing(self.state).pipeline, 'features2', 'result'),
        ]
        self.exe._execute_steps(steps, stage="parent")
