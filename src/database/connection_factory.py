"""
Database connection factory module.

This module provides a factory pattern to create the appropriate database
connection based on the configured database type (DuckDB or PostgreSQL).
"""
from .duckdb_connection import DuckDBConnection
from .postgresql_connection import PostgreSQLConnection
from .base import DatabaseConnection

"""
DuckDB connection module using SQLAlchemy.

This module provides a dedicated connection manager for DuckDB database
with proper session handling, connection pooling, and error management.
"""

import logging
from typing import Optional, Type

from sqlalchemy.orm import Session

from src.settings import settings


class DatabaseError(Exception):
    """ Defines a database connection error. Without extra details """


class CMSession:
    """ A context manager for database Sessions without support for async or connection/session pools.
        Can be used with any database type and as Depends at FastAPI e.g.
    """
    _current_connection: DatabaseConnection

    def __new__(cls, *args, **kwargs):
        if hasattr(cls, '_current_connection'):
            match settings.database.database_type:
                case 'duckdb':
                    db_connection = DuckDBConnection()
                case 'postgresql':
                    db_connection = PostgreSQLConnection()
                case x:
                    raise DatabaseError(f'Not supported database type: {x}')
            cls._current_connection = db_connection

        kwargs['db_connection'] = cls._current_connection

        instance = super().__new__(cls)
        instance.__init__(*args, **kwargs)
        return instance

    def __init__(self, db_connection: DatabaseConnection = DatabaseConnection()):
        self.db_connection = db_connection
        self.session: Session | None = None

    def __enter__(self):
        """
        Get a database session with proper context management.

        Returns:
            Session: SQLAlchemy session object

        Example:
            with db_connection.get_session() as session:
                result = session.execute("SELECT * FROM customers LIMIT 10")
                rows = result.fetchall()
        """
        self.session = self.db_connection.get_session()
        return self.session

    def __exit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc_val: Optional[BaseException],
            exc_tb: Optional[object]
    ):
        if exc_type:
            self.session.rollback()
            logging.error(f"Database session error: {exc_val}")
        else:
            self.session.commit()

        self.session.close()
