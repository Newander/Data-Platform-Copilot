import logging

from pydantic import BaseModel

from src.config import settings
from src.database.base_model import DatabaseObject


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
