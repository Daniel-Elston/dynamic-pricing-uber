from __future__ import annotations

import logging

from config import DataState
from src.data.make_dataset import MakeDataset


class DataPipeline:
    def __init__(self, data_state: DataState):
        self.ds = data_state

    def run_make_dataset(self):
        return MakeDataset(self.ds).pipeline()

    def main(self):
        logging.info('Starting Data Pipeline')
        try:
            self.run_make_dataset()
        except Exception as e:
            logging.exception(f'Error: {e}', exc_info=e)
            raise
        logging.info('Completed Data Pipeline')


if __name__ == '__main__':
    from main import setup
    setup()
    data_state = DataState()
    DataPipeline(data_state).main()
