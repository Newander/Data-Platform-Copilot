import abc
import logging
from typing import Any

from pydantic import BaseModel

from src.config import settings
from src.database.db_connector import ConnectionCM, ConnectionType


def create_all(cm_manager: ConnectionCM) -> None:
    """ Creating required tables and objects in the assigned database and default data """
    with cm_manager as connection:
        for db_cls in DatabaseObject.__subclasses__():
            db_instance = db_cls(connection)
            db_instance.execute_ddl()
            logging.info(f"{db_instance.name}: DDL executed")
            if default_data := db_instance.default_data():
                connection.execute(default_data)
                connection.commit()
                logging.info(f"{db_instance.name}: default data inserted")


class DatabaseObject[PartM: BaseModel, FullM: BaseModel](abc.ABC):
    """ Defines interface for required database objects (because SQLAlchemy & alembic support DuckDB badly) """
    default_schema = settings.database.default_schema

    name: str
    autoincrement: str

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

    def execute_ddl(self) -> None:
        raise NotImplementedError

    def default_data(self) -> str | None:
        ...


class NamespacePartModel(BaseModel):
    name: str


class NamespaceFullModel(NamespacePartModel):
    id: int


class Namespace(DatabaseObject):
    name = "namespace"

    @property
    def autoincrement(self) -> str:
        return f"{self.default_schema}.seq_namespace_id_autoincrement"

    def execute_ddl(self) -> None:
        for ddl in [
            f"""
                create table if not exists {self.default_schema}.{self.name}
                (
                    id   INTEGER PRIMARY KEY,
                    name VARCHAR(1024)
                )
            """,
            f""" CREATE SEQUENCE if not exists {self.autoincrement} START 1 """
        ]:
            self.connection.execute(ddl)
            logging.info(f"DDL executed: {ddl}")

        self.connection.commit()

    def all(self) -> list[NamespaceFullModel]:
        result_query = self.connection.execute(
            f"""
                select id, name
                from {settings.database.default_schema}.namespace
                order by id
            """
        ).fetchall()

        return [NamespaceFullModel(id=id_, name=name) for id_, name in result_query]

    def insert(self, model: NamespacePartModel) -> NamespaceFullModel:
        executed = self.connection.execute(
            f""" insert into {settings.database.default_schema}.namespace (id, name) 
                values (nextval('{self.autoincrement}'), ?) 
                returning id, name
            """,
            (model.name,)
        )
        result = executed.fetchone()
        return NamespaceFullModel(id=result[0], name=result[1])

    def get(self, id_: int) -> NamespaceFullModel | None:
        executed = self.connection.execute(
            f""" select id, name from {settings.database.default_schema}.namespace where id = ? """,
            (id_,)
        )
        if result := executed.fetchone():
            return NamespaceFullModel(id=result[0], name=result[1])
        return None

    def update(self, model: NamespaceFullModel) -> NamespaceFullModel:
        executed = self.connection.execute(
            f""" update {settings.database.default_schema}.namespace
                set name = ?
                where id = ?
                returning id, name
            """,
            (model.name, model.id)
        )
        result = executed.fetchone()
        return NamespaceFullModel(id=result[0], name=result[1])

    def delete(self, id_: int) -> None:
        self.connection.execute(
            f""" delete from {settings.database.default_schema}.namespace
                where id = ?
            """,
            (id_,)
        )
