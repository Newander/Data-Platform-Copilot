"""
Database package for multi-database connections via SQLAlchemy.

Supports both DuckDB and PostgreSQL connections based on configuration.
The database type is determined by the DATABASE_TYPE setting.
"""

# Import base protocol and exceptions
from .base import DatabaseConnection

# Import connection factory components
from .connection_factory import DatabaseError, CMSession

# Import specific connection classes for direct access if needed
from .duckdb_connection import DuckDBConnection
from .postgresql_connection import PostgreSQLConnection

__all__ = [
    "DatabaseConnection",
    "DatabaseError",
    "CMSession",
    "DuckDBConnection", 
    "PostgreSQLConnection"
]