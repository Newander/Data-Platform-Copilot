"""
DuckDB connection module using SQLAlchemy.

This module provides a dedicated connection manager for DuckDB database
with proper session handling, connection pooling, and error management.
"""

import logging
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, Optional, Protocol

from sqlalchemy import create_engine, Engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker, Session
from tenacity import retry, stop_after_attempt, wait_exponential

from src.settings import DB_DIR, DB_FILE_NAME


class DBConnection(Protocol):
    """ Connection main protocol class with the overall interface
        The database to support:
            - PostgreSQL
            - DuckDB
    """


class DuckDBConnection:
    """
    DuckDB connection manager using SQLAlchemy.
    
    This class provides a singleton pattern for managing DuckDB connections
    with proper error handling and connection pooling.
    """

    _instance: Optional['DuckDBConnection'] = None
    _engine: Optional[Engine] = None
    _session_factory: Optional[sessionmaker] = None

    def __new__(cls) -> 'DuckDBConnection':
        """Ensure singleton pattern."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        """Initialize the DuckDB connection manager."""
        if self._engine is None:
            self._initialize_engine()

    def _initialize_engine(self) -> None:
        """Initialize the SQLAlchemy engine for DuckDB."""
        try:
            # Construct database path
            db_path = self._get_database_path()

            # Create DuckDB connection string
            # Format: duckdb:///path/to/database.db
            connection_string = f"duckdb:///{db_path}"

            logging.info(f"Initializing DuckDB engine with path: {db_path}")

            # Create engine with connection pooling and configurations
            self._engine = create_engine(
                connection_string,
                echo=False,  # Set to True for SQL query logging
                pool_pre_ping=True,  # Validate connections before use
                pool_recycle=3600,  # Recycle connections every hour
                connect_args={
                    "read_only": False,  # Allow read/write operations
                }
            )

            # Create session factory
            self._session_factory = sessionmaker(
                bind=self._engine,
                autocommit=False,
                autoflush=True
            )

            # Test the connection
            self._test_connection()

            logging.info("DuckDB engine initialized successfully")

        except Exception as e:
            logging.error(f"Failed to initialize DuckDB engine: {e}")
            raise

    def _get_database_path(self) -> Path:
        """Get the full path to the DuckDB database file."""
        if DB_DIR is None:
            raise ValueError("DB_DIR environment variable is not set")

        db_path = DB_DIR / DB_FILE_NAME

        # Ensure the directory exists
        db_path.parent.mkdir(parents=True, exist_ok=True)

        return db_path

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def _test_connection(self) -> None:
        """Test the database connection."""
        if self._engine is None:
            raise RuntimeError("Engine not initialized")

        try:
            with self._engine.connect() as conn:
                # Simple test query
                result = conn.execute(text("SELECT 1 as test")).fetchone()
                if result[0] != 1:
                    raise RuntimeError("Connection test failed")
        except SQLAlchemyError as e:
            logging.error(f"Database connection test failed: {e}")
            raise

    @property
    def engine(self) -> Engine:
        """Get the SQLAlchemy engine instance."""
        if self._engine is None:
            self._initialize_engine()
        return self._engine

    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """
        Get a database session with proper context management.
        
        Yields:
            Session: SQLAlchemy session object
            
        Example:
            with db_connection.get_session() as session:
                result = session.execute("SELECT * FROM customers LIMIT 10")
                rows = result.fetchall()
        """
        if self._session_factory is None:
            raise RuntimeError("Session factory not initialized")

        session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logging.error(f"Database session error: {e}")
            raise
        finally:
            session.close()

    def execute_query(self, query: str, params: Optional[dict] = None) -> list:
        """
        Execute a query and return results.
        
        Args:
            query: SQL query to execute
            params: Optional parameters for the query
            
        Returns:
            List of result rows
        """
        with self.get_session() as session:
            try:
                if params:
                    result = session.execute(text(query), params)
                else:
                    result = session.execute(text(query))
                return result.fetchall()
            except SQLAlchemyError as e:
                logging.error(f"Query execution error: {e}")
                raise

    def close(self) -> None:
        """Close the database connection and clean up resources."""
        if self._engine:
            self._engine.dispose()
            self._engine = None
            self._session_factory = None
            logging.info("DuckDB connection closed")


# Global instance
_db_connection: Optional[DuckDBConnection] = None


def get_engine() -> Engine:
    """
    Get the global DuckDB SQLAlchemy engine instance.
    
    Returns:
        Engine: SQLAlchemy engine for DuckDB
    """
    global _db_connection
    if _db_connection is None:
        _db_connection = DuckDBConnection()
    return _db_connection.engine


@contextmanager
def get_session() -> Generator[Session, None, None]:
    """
    Get a database session with proper context management.
    
    This is a convenience function that uses the global connection instance.
    
    Yields:
        Session: SQLAlchemy session object
        
    Example:
        from src.database import get_session
        
        with get_session() as session:
            result = session.execute("SELECT * FROM customers LIMIT 10")
            rows = result.fetchall()
    """
    global _db_connection
    if _db_connection is None:
        _db_connection = DuckDBConnection()

    with _db_connection.get_session() as session:
        yield session


def get_connection() -> DuckDBConnection:
    """
    Get the global DuckDB connection instance.
    
    Returns:
        DuckDBConnection: The global connection manager instance
    """
    global _db_connection
    if _db_connection is None:
        _db_connection = DuckDBConnection()
    return _db_connection


class CMSession:
    """ A context manager for database Sessions """
    def __new__(cls, *args, **kwargs):
        instance = super().__new__(cls)
        return instance

    def __init__(self):
        ...

    def __enter__(self):
        ...

    def __exit__(self, exc_type, exc_val, exc_tb):
        ...
