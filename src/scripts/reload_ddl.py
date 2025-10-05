import logging

from src.config import settings
from src.database import create_all
from src.database.db_connector import create_connection, ConnectionCM

logging.basicConfig(
    level=settings.logging.level,
    format=settings.logging.format,
    datefmt=settings.logging.datefmt,
    force=True,  # overrides existing logging configuration (useful for repeated runs)
)

if __name__ == '__main__':
    db_connection = create_connection()
    db_connection.test_db_connection()
    create_all(ConnectionCM(db_connection), with_drop=True)
