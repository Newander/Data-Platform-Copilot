import abc
import logging
from typing import Any, Callable, Annotated

from fastapi import Depends
from pydantic import BaseModel

from src.config import settings
from src.database.db_connector import ConnectionCM, ConnectionType, opened_connection



class DatabaseObject[PartM: BaseModel, FullM: BaseModel](abc.ABC):
    """ Defines interface for required database objects (because SQLAlchemy & alembic support DuckDB badly) """
    default_schema = settings.database.default_schema

    name: str
    autoincrement: str

    @property
    def autoincrement(self) -> str:
        return f"{self.default_schema}.seq_{self.name}_id_autoincrement"

    def __init__(self, connection: ConnectionType):
        self.connection = connection

    def insert(self, model: PartM) -> FullM:
        raise NotImplementedError

    def get(self, id_: Any) -> FullM | None:
        raise NotImplementedError

    def update(self, model: FullM) -> FullM:
        raise NotImplementedError

    def delete(self, id_: Any) -> None:
        raise NotImplementedError

    def all(self) -> list[FullM]:
        raise NotImplementedError

    def drop_ddl(self) -> None:
        raise NotImplementedError

    def execute_ddl(self) -> None:
        raise NotImplementedError

    def default_data(self) -> str | None:
        ...


def depends_object[T: DatabaseObject](model: type[T]) -> Callable[[ConnectionType], T]:
    """ Initialize the DatabaseObject with connection """

    def depends_object(
            connection: Annotated[ConnectionType, Depends(opened_connection)],
    ) -> T:
        """ Name must be the same because of how FastAPI Depends are working """
        return model(connection)

    return depends_object
