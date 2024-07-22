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

    def run_make_dataset(self, load_path, save_path):
        return MakeDataset(self.ds).pipeline(load_path, save_path)

    def run_initial_processor(self, load_path, save_path):
        return InitialProcessor(self.ds).pipeline(load_path, save_path)

    def run_build_features(self, load_path, save_path):
        return BuildFeatures(self.ds, self.dc).pipeline(load_path, save_path)

    def run_build_moving_averages(self, load_path, save_path):
        return BuildMovingAverages(self.ds, self.dc).pipeline(load_path, save_path)

    def run_build_bounds(self, load_path, save_path):
        return BuildBounds(self.ds, self.dc).pipeline(load_path, save_path)

    def main(self):
        logging.info('Starting Data Pipeline')
        try:
            self.run_make_dataset(self.ds.raw_path, self.ds.sdo_path)
            self.run_initial_processor(self.ds.sdo_path, self.ds.initial_process_path)
            self.run_build_features(self.ds.initial_process_path, self.ds.features_path)
            self.run_build_moving_averages(self.ds.features_path, self.ds.interim_data_path)
            self.run_build_bounds(self.ds.features_path, self.ds.interim_data_path)

        except Exception as e:
            logging.exception(f'Error: {e}', exc_info=e)
            raise
        logging.info('Completed Data Pipeline')


if __name__ == '__main__':
    from main import setup
    setup()
    data_state = DataState()
    DataPipeline(data_state).main()
