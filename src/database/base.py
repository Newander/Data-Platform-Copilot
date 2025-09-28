import logging
from typing import Protocol

from sqlalchemy import Engine, text, MetaData
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from tenacity import retry, stop_after_attempt, wait_exponential


class DatabaseConnection(Protocol):
    """ Connection main protocol class with the overall interface
        The databases to support:
            - PostgreSQL
            - DuckDB
    """
    # Protocolized attributes and methods
    engine: Engine

    def get_session(self) -> Session:
        ...

    # Real Functions
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def test_db_connection(self) -> None:
        """ Test the database connection. """
        try:
            with self.engine.connect() as conn:
                # Simple test query
                result = conn.execute(text("SELECT 1 as test")).fetchone()
                if result[0] != 1:
                    raise RuntimeError("Connection test failed")
        except SQLAlchemyError as e:
            logging.error(f"Database connection test failed: {e}")
            raise

    def create_all(self, metadata: MetaData) -> None:
        metadata.create_all(self.engine)
        logging.info("Database isi initialized")
