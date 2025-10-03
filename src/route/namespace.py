from typing import Annotated

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from src.config import settings
from src.database.db_connector import opened_connection, ConnectionType

namespace_router = APIRouter(prefix='/namespace')


class Message(BaseModel):
    message: str


class Namespace(BaseModel):
    name: str


class IDNamespace(Namespace):
    id: int


class NamespaceListResponse(BaseModel):
    message: str
    namespaces: list[Namespace]


@namespace_router.get('/')
def list_namespaces(
        connection=Depends(opened_connection)
) -> NamespaceListResponse:
    schema_names = connection.execute(
        f"""
        select name
        from {settings.database.default_schema}.namespace
        order by 1
        """
    ).fetchall()

    return NamespaceListResponse(
        message="OK" if schema_names else "No namespaces created",
        namespaces=[Namespace(name=schema_name) for schema_name, in schema_names]
    )


@namespace_router.post('/')
def create_namespace(
        connection: Annotated[ConnectionType, Depends(opened_connection)],
) -> IDNamespace:
    ...


@namespace_router.put('/{namespace_id}')
def edit_namespace(
        connection: Annotated[ConnectionType, Depends(opened_connection)],
        namespace_id: int,
) -> IDNamespace:
    ...


@namespace_router.delete('/{namespace_id}')
def delete_namespace(
        connection: Annotated[ConnectionType, Depends(opened_connection)],
        namespace_id: int,
) -> Message:
    # todo: remove also tables
    ...
