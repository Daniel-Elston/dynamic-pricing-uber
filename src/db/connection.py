from __future__ import annotations

import logging

import psycopg2.pool
from sqlalchemy import create_engine

from src.db.config import DatabaseState


class DatabaseConnection:
    def __init__(self, db_state: DatabaseState):
        self.admin_creds = db_state.admin_creds
        self.db_info = db_state.db_info

        self.engine, self.conn = self.create_my_engine()
        self.pgsql_pool = self.create_my_pool()
        self.log_database_info()

    def load_sql(self, path):
        with open(path, 'r') as file:
            return file.read()

    def create_my_pool(self):
        """Initialize connection pool"""
        pgsql_pool = psycopg2.pool.SimpleConnectionPool(
            1, 10,
            user=self.admin_creds["user"],
            password=self.admin_creds["password"],
            host=self.admin_creds["host"],
            port=self.admin_creds["port"],
            database=self.db_info["database"])
        return pgsql_pool

    def create_my_engine(self):
        """Get the database engine."""
        engine = create_engine(
            f'postgresql+psycopg2://{self.admin_creds["user"]}:{self.admin_creds["password"]}@{self.admin_creds["host"]}:{self.admin_creds["port"]}/{self.db_info["database"]}')
        conn = psycopg2.connect(
            f'dbname={self.db_info["database"]} user={self.admin_creds["user"]} host={self.admin_creds["host"]} password={self.admin_creds["password"]}')
        return engine, conn

    def log_database_info(self):
        """Log connection and database information."""
        conn = self.pgsql_pool.getconn()
        try:
            cur = conn.cursor()
            cur.execute("SELECT current_database();")
            db_name = cur.fetchone()[0]
            cur.execute("SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'pg_catalog');")
            tables = cur.fetchall()
            logging.info(f"Connected to database: {db_name}")
            logging.info("Tables in the database:")
            for schema, table in tables:
                logging.info(f"Schema: {schema}, Table: {table}")
        except (Exception, psycopg2.DatabaseError) as error:
            logging.error(f"Failed to fetch database information: {error}")
        finally:
            cur.close()
            self.pgsql_pool.putconn(conn)

    def close_pool(self):
        """Close the connection pool on exit."""
        self.pgsql_pool.closeall()
        logging.info("Connection pool closed.")
