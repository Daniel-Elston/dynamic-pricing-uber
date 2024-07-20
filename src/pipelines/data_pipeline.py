from __future__ import annotations

import logging
from pathlib import Path

import utils.my_globs as my_globs
from config import DataState


class DataPipeline:
    def __init__(self, data_state: DataState):
        self.ds = data_state
        self.load_path = Path(my_globs.project_config['path']['raw'])
        self.save_path = Path(my_globs.project_config['path']['checkpoints'])

    def run_make_dataset(self):
        pass

    def main(self):
        logging.info('Starting Data Pipeline')
        try:
            self.run_make_dataset()
        except Exception as e:
            logging.exception(f'Error: {e}', exc_info=e)
            raise
        logging.info('Completed Data Pipeline')


if __name__ == '__main__':
    DataPipeline().main()
