import abc
import logging
from typing import Any, Callable, Annotated

from fastapi import Depends
from pydantic import BaseModel

from src.config import settings
from src.database.db_connector import ConnectionCM, ConnectionType, opened_connection



class DatabaseObject[CreateM: BaseModel, FullM: BaseModel](abc.ABC):
    """ Defines interface for required database objects (because SQLAlchemy & alembic support DuckDB badly) """
    default_schema = settings.database.default_schema

    name: str
    autoincrement: str
    model: type[FullM]

    @property
    def autoincrement(self) -> str:
        return f"{self.default_schema}.seq_{self.name}_id_autoincrement"

    def __init__(self, connection: ConnectionType):
        self.connection = connection


    def insert(self, model: CreateM) -> FullM:
        short_fields = {
            field: field_value
            for field in self.fields()
            if (field_value := getattr(model, field, None))
        }
        sql = f""" 
            insert into {settings.database.default_schema}.{self.name} (id, {','.join(short_fields)}) 
            values (nextval('{self.autoincrement}'), {','.join(['?'] * len(short_fields))}) 
            returning {','.join(self.fields())}
        """
        logging.info(f"SQL: {sql}")
        cursor = self.connection.execute(sql, tuple(short_fields.values()))
        result = cursor.fetchone()
        return self.create_model_from_tuple(result)

    def get(self, id_: int) -> FullM | None:
        sql = f""" 
            select {','.join(self.fields())} 
            from {settings.database.default_schema}.namespace 
            where id = ? 
        """
        logging.info(f"SQL: {sql}")
        executed = self.connection.execute(sql, (id_,))
        if result := executed.fetchone():
            return self.create_model_from_tuple(result)

        return None

    def update(self, model: FullM) -> FullM:
        raise NotImplementedError

    def delete(self, id_: Any, is_cascade: bool = False) -> None:
        raise NotImplementedError

    def all(self) -> list[FullM]:
        raise NotImplementedError

    def drop_ddl(self) -> None:
        raise NotImplementedError

    def execute_ddl(self) -> None:
        raise NotImplementedError

    def default_data(self) -> str | None:
        ...

    def fields(self) -> tuple[str, ...]:
        return tuple(self.model.model_fields.keys())

    def create_model_from_tuple(self, row: tuple) -> FullM:
            return self.model.model_validate(
                {
                    field_name: row[i]
                    for i, field_name in enumerate(self.fields())
                }
            )


def depends_object[T: DatabaseObject](model: type[T]) -> Callable[[ConnectionType], T]:
    """ Initialize the DatabaseObject with connection """

    def depends_object(
            connection: Annotated[ConnectionType, Depends(opened_connection)],
    ) -> T:
        """ Name must be the same because of how FastAPI Depends are working """
        return model(connection)

    return depends_object
