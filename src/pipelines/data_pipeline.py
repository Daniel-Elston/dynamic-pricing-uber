from __future__ import annotations

import logging

from config.data import DataConfig
from config.data import DataState
from src.data.make_dataset import MakeDataset
from src.data.process import InitialProcessor
from src.features.build_datasets import BuildBounds
from src.features.build_datasets import BuildMovingAverages
from src.features.build_features import BuildFeatures
from src.visuals.visualize import BoundVisuals


class DataPipeline:
    def __init__(self, data_state: DataState, data_config: DataConfig):
        self.ds = data_state
        self.dc = data_config

    def run_make_dataset(self, load_path, save_path):
        return MakeDataset(self.dc).pipeline(load_path, save_path)

    def run_initial_processor(self, load_path, save_path):
        return InitialProcessor(self.dc).pipeline(load_path, save_path)

    def run_build_features(self, load_path, save_path):
        return BuildFeatures(self.ds, self.dc).pipeline(load_path, save_path)

    def run_build_moving_averages(self, load_path, save_path):
        return BuildMovingAverages(self.ds, self.dc).pipeline(load_path, save_path)

    def run_build_bounds(self, load_path, save_path, result_path):
        return BuildBounds(self.ds, self.dc).pipeline(load_path, save_path, result_path)

    def run_bound_visuals(self, load_path):
        return BoundVisuals.bound_hours(load_path)

    def main(self):
        logging.info(
            'Starting Data Pipeline')
        try:
            pth = self.ds.paths
            self.run_make_dataset(pth['raw'], pth['sdo'])
            self.run_initial_processor(pth['sdo'], pth['process1'])
            self.run_build_features(pth['process1'], pth['features'])
            self.run_build_moving_averages(pth['features'], pth['interim'])
            self.run_build_bounds(pth['features'], pth['interim'], pth['result1'])

            # self.run_bound_visuals(pth['interim'])

        except Exception as e:
            logging.exception(f'Error: {e}', exc_info=e)
            raise
        logging.info(
            'Completed Data Pipeline')


if __name__ == '__main__':
    from main import setup
    setup()
    data_state = DataState()
    DataPipeline(data_state).main()
