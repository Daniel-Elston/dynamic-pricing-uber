from __future__ import annotations

import logging
from io import StringIO

import pandas as pd
import psycopg2.pool

from src.db.config import DatabaseState
from src.db.connection import DatabaseConnection


class DataHandler:
    def __init__(self, db_state: DatabaseState, db_connection: DatabaseConnection):
        self.conn = db_connection.conn
        self.pgsql_pool = db_connection.pgsql_pool
        self.db_info = db_state.db_info
        self.batch_size = db_state.batch_size
        self.chunk_size = db_state.chunk_size
        self.schema = db_state.schema
        self.table = db_state.table

    def copy_from_stringio(self, conn, df, table):
        """Using COPY command to bulk load data."""
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)
        cursor = conn.cursor()
        try:
            cursor.execute(f"SET search_path TO {self.schema};")
            cursor.copy_from(buffer, table, sep=",", null="")
            conn.commit()
            logging.info(f"SUCCESS: Data copied to {table} using COPY command.")
        except (Exception, psycopg2.DatabaseError) as error:
            conn.rollback()
            logging.error(f"Failed to copy data using COPY command: {error}")

    def insert_batches_to_db(self, data):
        """Insert data into the database in batches using COPY command."""
        conn = self.pgsql_pool.getconn()
        try:
            num_rows = len(data)
            for start in range(0, num_rows, self.batch_size):
                end = start + self.batch_size
                batch_data = data.iloc[start:end]
                logging.info(
                    f"Inserting batch {start // self.batch_size + 1}: rows {start} to {end-1}")
                self.copy_from_stringio(conn, batch_data, self.table)
        except (Exception, psycopg2.DatabaseError) as error:
            logging.error(f"Failed to insert batches: {error}")
        finally:
            self.pgsql_pool.putconn(conn)

    def fetch_data(self, filepath, query=None, chunk_size=5_000_000):
        conn = self.pgsql_pool.getconn()
        total_rows_fetched = 0
        chunk_count = 1
        filepath = str(filepath)

        accumulated_data = []

        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            if query is None:
                query = f"""SELECT * FROM {self.table};"""
            cur.execute(query)

            while True:
                rows = cur.fetchmany(self.batch_size)
                if not rows:
                    break

                data = [dict(row) for row in rows]
                accumulated_data.extend(data)
                total_rows_fetched += len(data)

                if len(accumulated_data) >= chunk_size:
                    df = pd.DataFrame(accumulated_data[:chunk_size])
                    chunk_filepath = f"{filepath.rsplit('.', 1)[0]}_{chunk_count}.parquet"
                    df.to_parquet(chunk_filepath, index=False)

                    accumulated_data = accumulated_data[chunk_size:]
                    chunk_count += 1

                    logging.info(f"Saved {chunk_size} rows to {chunk_filepath}")
                logging.info(f"Total fetched {total_rows_fetched} rows")

            # Save remaining data
            if accumulated_data:
                df = pd.DataFrame(accumulated_data)
                chunk_filepath = f"{filepath.rsplit('.', 1)[0]}_{chunk_count}.parquet"
                df.to_parquet(chunk_filepath, index=False)
                logging.info(f"Saved remaining {len(accumulated_data)} rows to {chunk_filepath}")

            logging.info("Data fetched and stored in chunks successfully.")
        except (Exception, psycopg2.DatabaseError) as error:
            logging.error(f"Failed to fetch data: {error}")
        finally:
            cur.close()
            self.pgsql_pool.putconn(conn)

        return total_rows_fetched
