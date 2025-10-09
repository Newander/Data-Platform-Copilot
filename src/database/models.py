import logging
from datetime import datetime
from typing import Any

from pydantic import BaseModel

from src.config import settings
from src.database.base_model import DatabaseObject


class NamespaceNameModel(BaseModel):
    name: str


class NamespaceCreateModel(BaseModel):
    name: str
    schema_name: str


class NamespaceFullModel(BaseModel):
    """ Order is important! """
    id: int
    name: str
    schema_name: str
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
                    schema_name VARCHAR(1024),
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
    namespace_id: int
    table_name: str


class TableFullModel(BaseModel):
    id: int
    namespace_id: int
    name: str
    table_name: str
    file_name: str | None
    file_size: int | None
    is_loaded: bool = False
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
                    name VARCHAR(1024) NOT NULL,
                    table_name VARCHAR(1024) NOT NULL,
                    file_name VARCHAR(1024),
                    file_size INTEGER,
                    is_loaded BOOLEAN DEFAULT FALSE,
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
