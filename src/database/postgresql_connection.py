"""
PostgreSQL connection module using SQLAlchemy.

This module provides a dedicated connection manager for PostgreSQL database
with proper session handling, connection pooling, and error management.
"""

import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from .base import DatabaseConnection
from src.settings import DB_HOST, DB_PORT, DB_DB, DB_USER, DB_PASSWORD, DB_SCHEMA, DB_DRIVER


class PostgreSQLConnection(DatabaseConnection):
    """
    PostgreSQL connection manager using SQLAlchemy.
    
    This class provides a singleton pattern for managing PostgreSQL connections
    with proper error handling and connection pooling.
    """

    def __init__(self):
        """Initialize the PostgreSQL connection manager."""
        # Create PostgreSQL connection string
        # Format: postgresql://user:password@host:port/database
        connection_string = self.build_connection_string()

        logging.info(f"Initializing PostgreSQL engine for {DB_HOST}:{DB_PORT}/{DB_DB}")

        # Create engine with connection pooling and configurations
        self.engine = create_engine(
            connection_string,
            echo=False,  # Set to True for SQL query logging
            pool_pre_ping=True,  # Validate connections before use
            pool_recycle=3600,  # Recycle connections every hour
            pool_size=10,  # Number of connections to maintain in pool
            max_overflow=20,  # Additional connections beyond pool_size
            connect_args={
                "options": f"-csearch_path={DB_SCHEMA}",  # Set default schema
            } if DB_SCHEMA else {}
        )

        # Create session factory
        self.session_factory = sessionmaker(
            bind=self.engine,
            autocommit=False,
            autoflush=True
        )

        # Test the connection
        self.test_db_connection()

        logging.info("PostgreSQL engine initialized successfully")

    @staticmethod
    def build_connection_string() -> str:
        """Build PostgreSQL connection string."""
        if not DB_PASSWORD:
            raise ValueError("PostgreSQL password is required")

        return f"{DB_DRIVER}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_DB}"

    def get_session(self) -> Session:
        """
        Get a database session with automatic cleanup.
        
        Yields:
            Session: SQLAlchemy session instance
            
        Raises:
            RuntimeError: If session factory is not initialized
            SQLAlchemyError: If database operations fail
        """
        if self.session_factory is None:
            raise RuntimeError("Session factory not initialized")

        return self.session_factory()
