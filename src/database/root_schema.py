import logging

from pydantic import BaseModel

from src.config import settings
from src.database.db_connector import ConnectionCM, ConnectionType


def create_all(cm_manager: ConnectionCM) -> None:
    """ Creating required tables and objects in the assigned database and default data """
    with cm_manager as connection:
        for db_cls in DatabaseObject.__subclasses__():
            db_instance = db_cls(connection)
            db_instance.ddl()
            logging.info(f"{db_instance.name}: DDL executed")
            if default_data := db_instance.default_data():
                connection.execute(default_data)
                connection.commit()
                logging.info(f"{db_instance.name}: default data inserted")


class DatabaseObject:
    """ Defines interface for required database objects (because SQLAlchemy & alembic support DuckDB badly) """
    default_schema = settings.database.default_schema

    name: str
    autoincrement: str

    def __init__(self, connection: ConnectionType):
        self.connection = connection

    def insert(self, model: BaseModel) -> BaseModel:
        raise NotImplementedError

    def all(self) -> list[BaseModel]:
        raise NotImplementedError

    def ddl(self) -> None:
        raise NotImplementedError

    def default_data(self) -> str | None:
        ...


class Namespace(DatabaseObject):
    name = "namespace"

    @property
    def autoincrement(self):
        return f"{self.default_schema}.seq_namespace_id_autoincrement"

    def ddl(self) -> None:
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

    class EditModel(BaseModel):
        name: str

    class IDModel(EditModel):
        id: int

    def all(self) -> list[IDModel]:
        result_query = self.connection.execute(
            f"""
                select id, name
                from {settings.database.default_schema}.namespace
                order by id
            """
        ).fetchall()

        return [Namespace.IDModel(id=id_, name=name) for id_, name in result_query]
