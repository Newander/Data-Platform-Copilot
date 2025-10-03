import logging

from src.database.db_connector import ConnectionCM
from src.config import settings


def create_all(cm_manager: ConnectionCM) -> None:
    """ Creating required tables and objects in the assigned database and default data """
    default_schema = settings.database.default_schema
    with cm_manager as connection:
        for db_cls in DatabaseObject.__subclasses__():
            db_instance = db_cls()
            ddl = db_instance.ddl(default_schema)
            connection.execute(ddl)
            logging.info(f"{db_instance.name}: DDL executed: {ddl}")
            if default_data := db_instance.default_data(default_schema):
                connection.execute(default_data)
                logging.info(f"{db_instance.name}: default data inserted")


class DatabaseObject:
    """ Defines interface for required database objects (because SQLAlchemy & alembic support DuckDB badly) """
    name: str

    def ddl(self, default_schema: str) -> str:
        raise NotImplementedError

    def default_data(self, default_schema: str) -> str | None:
        ...


class Namespace(DatabaseObject):
    name = "namespace"

    def ddl(self, default_schema: str) -> str:
        return f"""
            create table if not exists {default_schema}.{self.name}
            (
                id   INTEGER,
                name VARCHAR(1024)
            )
        """
