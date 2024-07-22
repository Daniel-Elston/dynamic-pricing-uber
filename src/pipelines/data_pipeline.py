from __future__ import annotations

import logging

from config import DataConfig
from config import DataState
from src.data.make_dataset import MakeDataset
from src.data.process import InitialProcessor
from src.features.build_datasets import BuildBounds
from src.features.build_datasets import BuildMovingAverages
from src.features.build_features import BuildFeatures


class DataPipeline:
    def __init__(self, data_state: DataState, data_config: DataConfig):
        self.ds = data_state
        self.dc = data_config

    def run_make_dataset(self):
        return MakeDataset(self.ds).pipeline()

    def run_initial_processor(self):
        return InitialProcessor(self.ds).pipeline()

    def run_build_features(self):
        return BuildFeatures(self.ds, self.dc).pipeline()

    def run_build_moving_averages(self):
        return BuildMovingAverages(self.ds, self.dc).pipeline()

    def run_build_bounds(self):
        return BuildBounds(self.ds, self.dc).pipeline()

    def main(self):
        logging.info('Starting Data Pipeline')
        try:
            self.run_make_dataset()
            self.run_initial_processor()
            self.run_build_features()
            self.run_build_moving_averages()
            self.run_build_bounds()

        except Exception as e:
            logging.exception(f'Error: {e}', exc_info=e)
            raise
        logging.info('Completed Data Pipeline')


if __name__ == '__main__':
    from main import setup
    setup()
    data_state = DataState()
    DataPipeline(data_state).main()
