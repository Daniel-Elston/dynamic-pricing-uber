from __future__ import annotations

import json
import logging
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


class FileHandler:
    def __init__(self):
        pass

    def save_json(self, data, path):
        with open(path, 'w') as file:
            json.dump(data, file)

    def load_json(self, path):
        with open(path, 'r') as file:
            return json.load(file)

    def save_parquet(self, df, filepath):
        table = pa.Table.from_pandas(df)
        pq.write_table(table, filepath)

    def load_parquet(self, filepath):
        table = pq.read_table(filepath)
        return table.to_pandas()

    def save_checkpoint(self, df, checkpoint):
        path = Path(f'data/checkpoints/{checkpoint}.parquet')
        logging.info(f'Saving checkpoint to {path}')
        self.save_to_parquet(df, path)

    def load_checkpoint(self, checkpoint):
        path = Path(f'data/checkpoints/{checkpoint}.parquet')
        logging.info(f'Loading checkpoint from {path}')
        return self.load_from_parquet(path)


if __name__ == '__main__':
    fh = FileHandler()
    filename = 'checkpoint1'
    path = Path(f'data/checkpoints/{filename}.parquet')
    print(fh.load_from_parquet('max_min_timestamps.parquet'))
