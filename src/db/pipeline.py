from __future__ import annotations

import logging

import pandas as pd

from src.db.config import DatabaseState
from src.db.data_handling import DataHandler
from src.db.operations import DatabaseOperations


class InsertPipeline:
    """Insert Parquet dataset into the database in batches using COPY command."""

    def __init__(self, db_state: DatabaseState, db_operations: DatabaseOperations, data_handler: DataHandler):
        self.db_operations = db_operations
        self.data_handler = data_handler
        self.table = db_state.db_info['table']

    def run_make_dataset(self, insert_path):
        if insert_path.suffix == '.xlsx':
            return pd.read_excel(insert_path)
        elif insert_path.suffix == '.csv':
            return pd.read_csv(insert_path)
        elif insert_path.suffix == '.parquet':
            return pd.read_parquet(insert_path)
        elif insert_path.suffix == '.parq':
            return pd.read_parquet(insert_path)
        else:
            raise ValueError(f'Unsupported file type: {insert_path.suffix}')

    def insert_data(self, data):
        self.db_operations.create_table_if_not_exists(data)
        self.data_handler.insert_batches_to_db(data)

    def run(self, insert_path):
        logging.info('Starting Data Pipeline')
        try:
            data = self.run_make_dataset(insert_path)
            self.insert_data(data)
        except Exception as e:
            logging.exception(f'Error: {e}', exc_info=e)
            raise
        logging.info('Completed Data Pipeline')


class FetchPipeline:
    """Fetch dataset from the database and save as Parquet."""

    def __init__(self, db_state: DatabaseState, db_operations: DatabaseOperations, data_handler: DataHandler):
        self.db_operations = db_operations
        self.data_handler = data_handler
        self.table = db_state.db_info['table']

    def fetch_data(self, filepath, query=None):
        fetched_data = self.data_handler.fetch_data(filepath, query=query)
        return fetched_data

    def run(self, fetch_path):
        logging.info('Starting Data Pipeline')
        try:
            # query = FileHandler().load_sql(Path('database/query.sql'))
            query = f"""
            SELECT * FROM {self.table};
            """
            self.fetch_data(fetch_path, query=query)
        except Exception as e:
            logging.exception(f'Error: {e}', exc_info=e)
            raise
        logging.info('Completed Data Pipeline')
