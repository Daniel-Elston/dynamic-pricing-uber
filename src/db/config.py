from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from dataclasses import field
from pprint import pformat


def db_creds():
    admin_conf = {
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'host': os.getenv('POSTGRES_HOST'),
        'port': os.getenv('POSTGRES_PORT')
    }
    db_conf = {
        'database': os.getenv('POSTGRES_DB'),
        'schema': os.getenv('POSTGRES_SCHEMA'),
        'table': os.getenv('POSTGRES_TABLE')
    }
    return admin_conf, db_conf


@dataclass
class DatabaseState:
    insert_file: str = os.getenv('INSERT_FILE')
    fetch_file: str = os.getenv('FETCH_FILE')
    batch_size: int = 100000
    chunk_size: int = 5_000_000

    admin_creds: dict = field(init=False)
    db_info: dict = field(init=False)

    database: str = field(init=False)
    schema: str = field(init=False)
    table: str = field(init=False)

    def __post_init__(self):
        self.admin_creds, self.db_info = db_creds()
        self.database = self.db_info['database']
        self.schema = self.db_info['schema']
        self.table = self.db_info['table']
        post_init_dict = {
            'admin_creds': self.admin_creds,
            'db_info': self.db_info
        }
        logging.debug(f"Initialized DataState: {pformat(post_init_dict)}")

    def __repr__(self):
        return pformat(self.__dict__)
