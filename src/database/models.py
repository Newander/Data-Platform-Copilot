import logging
from datetime import datetime
from typing import Any

from pydantic import BaseModel

from src.config import settings
from src.database.base_model import DatabaseObject


class NamespacePartModel(BaseModel):
    name: str


class NamespaceFullModel(BaseModel):
    """ Order is important! """
    id: int
    name: str
    created_at: datetime
    updated_at: datetime | None


class Namespace(DatabaseObject):
    name = "namespace"
    model = NamespaceFullModel

    def drop_ddl(self) -> None:
        self.connection.execute(
            f""" drop table if exists {self.default_schema}.{self.name} cascade """
        )
        self.connection.commit()

    def execute_ddl(self) -> None:
        ddl_list = []
        ddl_list.extend([
            f"""
                create table if not exists {self.default_schema}.{self.name}
                (
                    id   INTEGER PRIMARY KEY,
                    name VARCHAR(1024),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            f""" CREATE SEQUENCE if not exists {self.autoincrement} START 1 """,
        ])
        for ddl in ddl_list:
            self.connection.execute(ddl)
            logging.info(f"DDL executed: {ddl}")

        self.connection.commit()

    def all(self) -> list[NamespaceFullModel]:
        result_query = self.connection.execute(
            f"""
                select {','.join(self.fields())}
                from {settings.database.default_schema}.namespace
                order by id
            """
        ).fetchall()

        return [
            self.create_model_from_tuple(fields)
            for fields in result_query
        ]

    def get(self, id_: int) -> NamespaceFullModel | None:
        executed = self.connection.execute(
            f""" 
                select {','.join(self.fields())} 
                from {settings.database.default_schema}.namespace 
                where id = ? 
            """,
            (id_,)
        )
        if result := executed.fetchone():
            return self.create_model_from_tuple(result)

        return None

    def update(self, model: NamespaceFullModel) -> NamespaceFullModel:
        executed = self.connection.execute(
            f""" update {settings.database.default_schema}.namespace
                set name = ?, updated_at = CURRENT_TIMESTAMP
                where id = ?
                returning id, name
            """,
            (model.name, model.id)
        )
        result = executed.fetchone()
        return NamespaceFullModel(id=result[0], name=result[1])

    def delete(self, id_: int, is_cascade: bool = False) -> None:
        if is_cascade:
            self.connection.execute(
                f""" delete from {settings.database.default_schema}.namespace_table
                    where namespace_id = ?
                """,
                (id_,)
            )
        self.connection.execute(
            f""" delete from {settings.database.default_schema}.namespace
                where id = ?
            """,
            (id_,)
        )


class TablePartModel(BaseModel):
    name: str


class TableFullModel(TablePartModel):
    id: int
    namespace_id: int
    file_name: str | None
    is_loaded: bool | None
    created_at: datetime
    updated_at: datetime | None


class Table(DatabaseObject):
    name: str = "namespace_table"
    model = TableFullModel

    def drop_ddl(self) -> None:
        self.connection.execute(
            f""" drop table if exists {self.default_schema}.{self.name} cascade """
        )
        self.connection.commit()

    def execute_ddl(self) -> None:
        ddl_list = []
        ddl_list.extend([
            f"""
                create table if not exists {self.default_schema}.{self.name}
                (
                    id   INTEGER PRIMARY KEY,
                    namespace_id INTEGER NOT NULL,
                    name VARCHAR(1024),
                    file_name VARCHAR(1024),
                    is_loaded BOOLEAN,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (namespace_id) 
                        REFERENCES {self.default_schema}.{Namespace.name}(id)
                )
            """,
            f""" CREATE SEQUENCE if not exists {self.autoincrement} START 1 """,
        ])
        for ddl in ddl_list:
            logging.info(f"DDL executing: {ddl}")
            self.connection.execute(ddl)

        self.connection.commit()

    def get(self, id_: Any) -> TableFullModel | None:
        executed = self.connection.execute(
            f""" 
            select id, namespace_id, name, file_name, is_loaded, created_at, updated_at 
            from {settings.database.default_schema}.{self.name} 
            where id = ? """,
            (id_,)
        )
        if result := executed.fetchone():
            id_, namespace_id, name, file_name, is_loaded, created_at, updated_at = result
            return TableFullModel(
                id=id_,
                namespace_id=namespace_id,
                name=name,
                file_name=file_name,
                is_loaded=is_loaded,
                created_at=created_at,
                updated_at=updated_at
            )

        return None
