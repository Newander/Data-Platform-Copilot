from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI

from src.database.db_connector import create_connection, ConnectionCM
from src.database import create_all


@asynccontextmanager
async def lifespan_routine(app: FastAPI) -> AsyncIterator[None]:
    """put any other startup init here (DB pools, caches, etc.)"""
    db_connection = create_connection()
    db_connection.test_db_connection()

    try:
        yield
    finally:
        # --- shutdown ---
        ...
        # close other resources here
