from __future__ import annotations

import logging

import pandas as pd
import psycopg2.pool

from src.db.config import DatabaseState
from src.db.connection import DatabaseConnection


class DatabaseOperations:
    def __init__(self, db_state: DatabaseState, db_connection: DatabaseConnection):
        self.conn = db_connection.conn
        self.pgsql_pool = db_connection.pgsql_pool
        self.db_info = db_state.db_info

    def create_table_if_not_exists(self, data):
        """Create table if it does not exist."""
        conn = self.pgsql_pool.getconn()
        try:
            cur = conn.cursor()
            cur.execute(f"SET search_path TO {self.db_info['schema']}")
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.db_info['table']} (
                {', '.join([f"{col} {self._map_dtype(dtype)}" for col, dtype in zip(data.columns, data.dtypes)])});
            """
            logging.info(f"Creating table with SQL: {create_table_sql}")
            cur.execute(create_table_sql)
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            conn.rollback()
            logging.error(f"Failed to create table: {error}")
        finally:
            cur.close()
            self.pgsql_pool.putconn(conn)

    def _map_dtype(self, dtype):
        """Map pandas/numpy dtype to SQL dtype."""
        if pd.api.types.is_integer_dtype(dtype):
            return "BIGINT"
        elif pd.api.types.is_float_dtype(dtype):
            return "FLOAT"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return "TIMESTAMP"
        else:
            return "TEXT"
