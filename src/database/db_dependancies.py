from functools import wraps
from typing import Annotated, Callable

from duckdb import DuckDBPyConnection
from fastapi import Depends

from src.database.db_connector import ConnectionType, opened_connection
from src.database.root_schema import DatabaseObject


def depends_object[T: DatabaseObject](model: type[T]) -> Callable[[DuckDBPyConnection], T]:
    """ Initialize the DatabaseObject with connection """
    def depends_object(
        connection: Annotated[ConnectionType, Depends(opened_connection)],
    ) -> T:
        """ Name must be the same because of how FastAPI Depends are working """
        return model(connection)

    return depends_object
