import logging
from pathlib import Path
from typing import Optional, Any, Dict, Protocol
from typing import Type
from urllib.parse import urlparse

import duckdb
import psycopg2 as psycopg
from sqlalchemy import Engine
from sqlalchemy.orm import Session

from src.settings import settings
from .duckdb_connection import DuckDBConnection
from .postgresql_connection import PostgreSQLConnection

logger = logging.getLogger(__name__)


class DatabaseConnection:
    """ Connection main protocol class with the overall interface
        The databases to support:
            - PostgreSQL
            - DuckDB
    """
    dsn: str

    def create_connection(self) -> None:
        raise NotImplementedError

    def test_db_connection(self) -> None:
        raise NotImplementedError


class DuckDBContextManager:
    """Simple context manager for DuckDB connections."""

    _instance: Optional['DuckDBContextManager'] = None

    def __new__(cls) -> 'DuckDBContextManager':
        """Ensure singleton pattern."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.__init__()

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

        logger.debug(f"DuckDB connection established: {self.dsn or 'in-memory'}")

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exit the context and cleanup connection.
        
        Args:
            exc_type: Exception type if an exception occurred
            exc_val: Exception value if an exception occurred  
            exc_tb: Exception traceback if an exception occurred
        """
        if self.connection:
            try:
                self.connection.close()
                logger.debug("DuckDB connection closed")
            except Exception as e:
                logger.error(f"Error closing DuckDB connection: {e}")
            finally:
                self.connection = None

        if exc_type:
            logger.error(f"Exception in DuckDB context: {exc_val}")


class PostgreSQLContextManager:
    """Simple context manager for PostgreSQL connections using psycopg."""

    def __init__(
            self,
            dsn_string: Optional[str] = None,
            autocommit: bool = False,
            **connection_kwargs
    ):
        """
        Initialize PostgreSQL context manager.
        
        Args:
            connection_string: Full PostgreSQL connection string (DSN).
                              If provided, other connection parameters are ignored.
            host: PostgreSQL server host
            port: PostgreSQL server port
            database: Database name
            user: Database user
            password: Database password
            autocommit: Whether to enable autocommit mode
            **connection_kwargs: Additional psycopg connection parameters
        """
        self.autocommit = autocommit
        self.connection: Optional[psycopg.Connection] = None
        self.connection_params = {"conninfo": dsn_string}

    def __enter__(self) -> psycopg.Connection:
        """
        Enter the context and establish PostgreSQL connection.
        
        Returns:
            psycopg.Connection: Active PostgreSQL connection
        """
        try:
            self.connection = psycopg.connect(**self.connection_params)

            if self.autocommit:
                self.connection.autocommit = True

            logger.debug(f"PostgreSQL connection established: {self.connection_params.get('host', 'DSN')}")
            return self.connection

        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exit the context and cleanup connection.
        
        Args:
            exc_type: Exception type if an exception occurred
            exc_val: Exception value if an exception occurred  
            exc_tb: Exception traceback if an exception occurred
        """
        if self.connection:
            try:
                if exc_type and not self.autocommit:
                    # Rollback transaction on exception
                    self.connection.rollback()
                    logger.debug("PostgreSQL transaction rolled back due to exception")
                elif not self.autocommit:
                    # Commit transaction on successful completion
                    self.connection.commit()
                    logger.debug("PostgreSQL transaction committed")

                self.connection.close()
                logger.debug("PostgreSQL connection closed")
            except Exception as e:
                logger.error(f"Error closing PostgreSQL connection: {e}")
            finally:
                self.connection = None

        if exc_type:
            logger.error(f"Exception in PostgreSQL context: {exc_val}")


def get_duckdb_connection(dsn: Optional[str] = None, read_only: bool = False) -> DuckDBContextManager:
    """
    Factory function to create DuckDB context manager.
    
    Args:
        dsn: Path to DuckDB database file. If None, creates in-memory database.
        read_only: Whether to open database in read-only mode.
        
    Returns:
        DuckDBContextManager: Context manager for DuckDB connection
        
    Example:
        # File-based database
        with get_duckdb_connection("data/my_db.duckdb") as conn:
            result = conn.execute("SELECT * FROM my_table").fetchall()
            
        # In-memory database
        with get_duckdb_connection() as conn:
            conn.execute("CREATE TABLE test (id INTEGER, name VARCHAR)")
            conn.execute("INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob')")
            result = conn.execute("SELECT * FROM test").fetchall()
    """
    return DuckDBContextManager(dsn, read_only)


def get_postgresql_connection(
        connection_string: Optional[str] = None,
        host: str = "localhost",
        port: int = 5432,
        database: str = "postgres",
        user: str = "postgres",
        password: Optional[str] = None,
        autocommit: bool = False,
        **kwargs
) -> PostgreSQLContextManager:
    """
    Factory function to create PostgreSQL context manager.
    
    Args:
        connection_string: Full PostgreSQL connection string (DSN).
                          Format: 'postgresql://user:password@host:port/database'
        host: PostgreSQL server host
        port: PostgreSQL server port  
        database: Database name
        user: Database user
        password: Database password
        autocommit: Whether to enable autocommit mode
        **kwargs: Additional psycopg connection parameters
        
    Returns:
        PostgreSQLContextManager: Context manager for PostgreSQL connection
        
    Example:
        # Using individual parameters
        with get_postgresql_connection(
            host="localhost",
            port=5432,
            database="mydb", 
            user="myuser",
            password="mypass"
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM users")
                result = cur.fetchall()
                
        # Using connection string
        with get_postgresql_connection(
            connection_string="postgresql://user:pass@localhost:5432/mydb"
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO logs (message) VALUES (%s)", ("Hello PostgreSQL!",))
                
        # Autocommit mode
        with get_postgresql_connection(
            host="localhost", 
            database="mydb",
            user="myuser", 
            password="mypass",
            autocommit=True
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE TABLE test (id SERIAL PRIMARY KEY, name TEXT)")
    """
    return PostgreSQLContextManager(
        connection_string=connection_string,
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
        autocommit=autocommit,
        **kwargs
    )


def parse_postgresql_url(url: str) -> Dict[str, Any]:
    """
    Parse PostgreSQL URL into connection parameters.
    
    Args:
        url: PostgreSQL URL in format 'postgresql://user:password@host:port/database'
        
    Returns:
        Dict[str, Any]: Dictionary with connection parameters
        
    Example:
        params = parse_postgresql_url('postgresql://user:pass@localhost:5432/mydb')
        with get_postgresql_connection(**params) as conn:
            # use connection
    """
    parsed = urlparse(url)

    params = {
        "host": parsed.hostname or "localhost",
        "port": parsed.port or 5432,
        "database": parsed.path.lstrip('/') if parsed.path else "postgres",
        "user": parsed.username or "postgres",
    }

    if parsed.password:
        params["password"] = parsed.password

    return params


def create_connection() -> DatabaseConnection:
    match settings.database.database_type:
        case 'duckdb':
            db_connection = DuckDBConnection()
        case 'postgresql':
            db_connection = PostgreSQLConnection()
        case x:
            raise DatabaseError(f'Not supported database type: {x}')

    return db_connection


class DatabaseError(Exception):
    """ Defines a database connection error. Without extra details """


class CMSession:
    """ A context manager for database Sessions without support for async or connection/session pools.
        Can be used with any database type and as Depends at FastAPI e.g.
    """
    _current_connection: DatabaseConnection

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_current_connection'):
            cls._current_connection = create_connection()

        kwargs['db_connection'] = cls._current_connection

        instance = super().__new__(cls)
        instance.__init__(*args, **kwargs)
        return instance

    def __init__(self, db_connection: DatabaseConnection):
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
