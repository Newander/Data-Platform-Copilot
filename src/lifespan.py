from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI

from src.database.connection_factory import create_connection
from src.database.models import CommonMetadata


@asynccontextmanager
async def lifespan_routine(app: FastAPI) -> AsyncIterator[None]:
    # put any other startup init here (DB pools, caches, etc.)
    db_connection = create_connection()
    db_connection.create_all(CommonMetadata)

    try:
        yield
    finally:
        # --- shutdown ---
        ...
        # close other resources here
