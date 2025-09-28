"""
DuckDB connection module using SQLAlchemy.

This module provides a dedicated connection manager for DuckDB database
with proper session handling, connection pooling, and error management.
"""

import logging
from pathlib import Path
from typing import Optional

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from src.database.base import DatabaseConnection
from src.settings import DB_DIR, DB_FILE_NAME


class DuckDBConnection(DatabaseConnection):
    """
    DuckDB connection manager using SQLAlchemy.
    
    This class provides a singleton pattern for managing DuckDB connections
    with proper error handling and connection pooling.
    """

    _instance: Optional['DuckDBConnection'] = None

    def __new__(cls) -> 'DuckDBConnection':
        """Ensure singleton pattern."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.__init__()

        return cls._instance

    def __init__(self):
        """Initialize the DuckDB connection manager."""
        # Construct a database path
        db_path = self.get_database_path()

        logging.info(f"Initializing DuckDB engine with path: {db_path}")
        # Create an engine with connection pooling and configurations
        self.engine = create_engine(
            # Format: duckdb:///path/to/database.db
            f"duckdb:///{db_path}",
            echo=False,  # Set to True for SQL query logging
            pool_pre_ping=True,  # Validate connections before use
            pool_recycle=3600,  # Recycle connections every hour
            connect_args={
                "read_only": False,  # Allow read/write operations
            }
        )

        # Create a session factory
        self.session_factory = sessionmaker(
            bind=self.engine,
            autocommit=False,
            autoflush=True
        )

        # Test the connection
        self.test_db_connection()
        logging.info("DuckDB engine initialized successfully")

    @staticmethod
    def get_database_path() -> Path:
        """Get the full path to the DuckDB database file."""
        if DB_DIR is None:
            raise ValueError("DB_DIR environment variable is not set")

        db_path = DB_DIR / DB_FILE_NAME

        # Ensure the directory exists
        db_path.parent.mkdir(parents=True, exist_ok=True)

        return db_path

    def get_session(self) -> Session:
        """
        Get a database session with proper context management.
        
        Yields:
            Session: SQLAlchemy session object
            
        Example:
            with db_connection.get_session() as session:
                result = session.execute("SELECT * FROM customers LIMIT 10")
                rows = result.fetchall()
        """
        if self.session_factory is None:
            raise RuntimeError("Session factory not initialized")

        return self.session_factory()
