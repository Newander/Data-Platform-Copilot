from typing import Annotated

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from src.config import settings
from src.database.db_connector import opened_connection, ConnectionType
from src.database.root_schema import Namespace

namespace_router = APIRouter(prefix='/namespace')


class Message(BaseModel):
    message: str


class NamespaceListResponse(BaseModel):
    message: str
    namespaces: list[Namespace.IDModel]


@namespace_router.get('/')
def list_namespaces(
        connection: Annotated[ConnectionType, Depends(opened_connection)],
) -> NamespaceListResponse:
    namespace_obj = Namespace(connection)
    namespaces = namespace_obj.all()

    return NamespaceListResponse(
        message="OK" if namespaces else "No namespaces created",
        namespaces=namespaces
    )


@namespace_router.post('/')
def create_namespace(
        connection: Annotated[ConnectionType, Depends(opened_connection)],
        new_namespace: Namespace.EditModel,
) -> Namespace.IDModel:
    executed = connection.execute(
        f""" insert into {settings.database.default_schema}.namespace (name) values (?) returning id, name
        """,
        (new_namespace.name,)
    )
    result = executed.fetchone()
    return Namespace.IDModel(id=result[0], name=result[1])


@namespace_router.put('/{namespace_id}')
def edit_namespace(
        connection: Annotated[ConnectionType, Depends(opened_connection)],
        namespace_id: int,
) -> Namespace.IDModel:
    ...


@namespace_router.delete('/{namespace_id}')
def delete_namespace(
        connection: Annotated[ConnectionType, Depends(opened_connection)],
        namespace_id: int,
) -> Message:
    # todo: remove also tables
    ...
