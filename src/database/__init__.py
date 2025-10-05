""" Module is required for correctly working versioning tools """

import logging

from .base_model import DatabaseObject
from .db_connector import ConnectionCM
from .models import Namespace, NamespacePartModel, NamespacePartModel


def create_all(cm_manager: ConnectionCM, with_drop: bool = False) -> None:
    """ Creating required tables and objects in the assigned database and default data """
    with cm_manager as connection:
        for db_cls in DatabaseObject.__subclasses__():
            db_instance = db_cls(connection)
            db_instance.execute_ddl(with_drop)
            logging.info(f"{db_instance.name}: DDL executed")
            if default_data := db_instance.default_data():
                connection.execute(default_data)
                connection.commit()
                logging.info(f"{db_instance.name}: default data inserted")


__all__ = [
    "create_all",
    "ConnectionCM",
    "DatabaseObject",
    "Namespace", "NamespacePartModel", "NamespacePartModel"
]
