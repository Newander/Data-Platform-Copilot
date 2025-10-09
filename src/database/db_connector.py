import logging
from functools import lru_cache
from pathlib import Path
from typing import Optional

import duckdb
import psycopg2
from tenacity import retry, stop_after_attempt, wait_exponential

from src.config import settings

ConnectionType = duckdb.DuckDBPyConnection


class DatabaseConnection:
    """ Connection main protocol class with the overall interface
        The databases to support:
            - PostgreSQL
            - DuckDB
    """
    dsn: str
    connection: ConnectionType | None

    def create_connection(self) -> None:
        raise NotImplementedError

    def close_connection(self) -> None:
        raise NotImplementedError

    def handle_exception(self, exc: Exception | None) -> None:
        raise NotImplementedError

    def commit(self) -> None:
        pass

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def test_db_connection(self) -> None:
        """ Test the database connection. """
        try:
            # Simple test query
            result = self.connection.execute("SELECT 1 as test").fetchone()
            logging.info("Database is connected")
        except Exception as e:
            logging.error(f"Database connection test failed: {e}")
            raise
        if result[0] != 1:
            raise RuntimeError("Connection test failed")


class DuckDBContextManager(DatabaseConnection):
    """Simple context manager for DuckDB connections."""

    _instance: Optional['DuckDBContextManager'] = None

    def __new__(cls, *args, **kwargs) -> 'DuckDBContextManager':
        """Ensure singleton pattern."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.__init__(*args, **kwargs)

        return cls._instance

    def __init__(self, dsn: Optional[str] = None, read_only: bool = False):
        """
        Initialize DuckDB context manager.
        
        Args:
            dsn: Path to DuckDB database file. If None, creates in-memory database.
            read_only: Whether to open database in read-only mode.
        """
        self.dsn = dsn
        self.read_only = read_only
        self.connection: Optional[duckdb.DuckDBPyConnection] = None

        logging.info(f"Initializing DuckDB connection: {self.dsn or 'in-memory'}")

    def create_connection(self) -> None:
        """
        Enter the context and establish DuckDB connection.
        
        Returns:
            duckdb.DuckDBPyConnection: Active DuckDB connection
        """
        if self.dsn:
            # Ensure parent directory exists
            Path(self.dsn).parent.mkdir(parents=True, exist_ok=True)
            self.connection = duckdb.connect(
                database=self.dsn,
                read_only=self.read_only
            )
        else:
            # In-memory database
            self.connection = duckdb.connect()

        logging.debug(f"DuckDB connection established: {self.dsn or 'in-memory'}")

    def handle_exception(self, exc: Exception | None = None) -> None:
        pass

    def close_connection(self) -> None:
        if self.connection:
            try:
                self.connection.close()
                logging.debug("DuckDB connection closed")
            except Exception as e:
                logging.error(f"Error closing DuckDB connection: {e}")
            finally:
                self.connection = None


class PostgreSQLContextManager(DatabaseConnection):
    """Simple context manager for PostgreSQL connections using psycopg2.
        todo: test the class and set typings
    """

    def __init__(
            self,
            dsn_string: Optional[str] = None,
            autocommit: bool = False,
            **connection_kwargs
    ):
        self.autocommit = autocommit
        self.connection: Optional[psycopg2.Connection] = None
        self.connection_params = {"conninfo": dsn_string, **connection_kwargs}

    def create_connection(self):
        self.connection = psycopg2.connect(**self.connection_params)

        if self.autocommit:
            self.connection.autocommit = True

        logging.debug(f"PostgreSQL connection established: {self.connection_params.get('host', 'DSN')}")

    def handle_exception(self, exc: Exception | None = None) -> None:
        if exc and not self.autocommit:
            # Rollback transaction on exception
            self.connection.rollback()
            logging.debug("PostgreSQL transaction rolled back due to exception")

    def close_connection(self):
        if not self.autocommit:
            # Commit transaction on successful completion
            self.connection.commit()
            logging.debug("PostgreSQL transaction committed")

        self.connection.close()
        self.connection = None


class DatabaseError(Exception):
    """ Defines a database connection error. Without extra details """


@lru_cache(32)
def create_connection() -> DatabaseConnection:
    db_connection = None
    match settings.database.database_type:
        case 'duckdb':
            db_connection = DuckDBContextManager(
                dsn=settings.database.duck_db_path(),
                read_only=False
            )
        case 'postgresql':
            db_connection = PostgreSQLContextManager(
                dsn_string=settings.database.postgres_dsn(),
                autocommit=True,
            )
        case x:
            raise DatabaseError(f'Not supported database type: {x}')

    db_connection.create_connection()
    return db_connection


class ConnectionCM:
    """ A context manager for database Sessions without support for async or connection/session pools.
        Can be used with any database type and as Depends at FastAPI e.g.
    """
    _current_connection: DatabaseConnection

    def __new__(cls, db_connection: DatabaseConnection | None = None):
        if not hasattr(cls, '_current_connection'):
            cls._current_connection = db_connection or create_connection()

        instance = super().__new__(cls)
        instance.__init__(db_connection=db_connection or cls._current_connection)
        return instance

    def __init__(self, db_connection: DatabaseConnection | None = None):
        self.db_connection = db_connection or self._current_connection

    def __enter__(self) -> ConnectionType:
        if not self.db_connection:
            raise DatabaseError("Database connection not initialized")
        self.db_connection.create_connection()
        return self.db_connection.connection

    def __exit__(
            self,
            exc_type: Optional[BaseException],
            exc_val: Optional[BaseException],
            exc_tb: Optional[object]
    ):
        if exc_type:
            self.db_connection.handle_exception(exc_val)
            self.db_connection.close_connection()
            logging.error(f"Database session error: {exc_val}")
        else:
            self.db_connection.commit()


def opened_connection() -> ConnectionType:
    with ConnectionCM() as connection:
        return connection
