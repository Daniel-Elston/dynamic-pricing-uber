from __future__ import annotations

import logging

from src.pipelines.data_pipeline import DataPipeline


class TestPipeline(DataPipeline):
    def __init__(self, config):
        super().__init__(config)

    def load_data(self):
        pass

    def main(self):
        logging.info('Starting Data Pipeline')
        try:
            super().main()
        except Exception as e:
            logging.exception(f'Error: {e}', exc_info=e)
            raise
        logging.info('Completed Data Pipeline')
