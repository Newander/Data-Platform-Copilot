from typing import Annotated

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from src.database.base_model import depends_object
from src.database.db_connector import ConnectionType, opened_connection
from src.database.models import Namespace, NamespacePartModel, NamespaceFullModel
from src.route.inspect_schema import Message
from src.route.namespace_table import table_router, get_namespace_depends

namespace_router = APIRouter(prefix='/namespace')


class NamespaceListResponse(BaseModel):
    message: str
    namespaces: list[NamespaceFullModel]


class NamespaceResponse(BaseModel):
    message: str
    namespace: NamespaceFullModel | None


@namespace_router.get('/')
def list_namespaces(
        namespace_obj: Annotated[Namespace, Depends(depends_object(Namespace))],
) -> NamespaceListResponse:
    namespaces = namespace_obj.all()
    return NamespaceListResponse(
        message="OK" if namespaces else "No namespaces created",
        namespaces=namespaces
    )


@namespace_router.post('/')
def create_namespace(
        connection: Annotated[ConnectionType, Depends(opened_connection)],
        namespace_obj: Annotated[Namespace, Depends(depends_object(Namespace))],
        new_namespace: NamespacePartModel,
) -> NamespaceFullModel:
    connection.execute(f" CREATE SCHEMA IF NOT EXISTS {new_namespace.name} ")
    full_new_namespace = namespace_obj.insert(new_namespace)
    return full_new_namespace


@namespace_router.get('/{namespace_id}')
def get_namespace(
        namespace: Annotated[NamespaceFullModel, Depends(get_namespace_depends)],
) -> NamespaceFullModel:
    return namespace


@namespace_router.put('/{namespace_id}')
def edit_namespace(
        namespace_obj: Annotated[Namespace, Depends(depends_object(Namespace))],
        exist_namespace: Annotated[NamespaceFullModel, Depends(get_namespace_depends)],
        updated_namespace: NamespacePartModel,
) -> NamespaceFullModel:
    if exist_namespace == updated_namespace:
        return exist_namespace

    return namespace_obj.update(
        NamespaceFullModel(id=exist_namespace.id, **updated_namespace.model_dump())
    )


@namespace_router.delete('/{namespace_id}')
def delete_namespace(
        connection: Annotated[ConnectionType, Depends(opened_connection)],
        namespace_obj: Annotated[Namespace, Depends(depends_object(Namespace))],
        namespace: Annotated[NamespaceFullModel, Depends(get_namespace_depends)],
) -> Message:
    connection.execute(f" DROP SCHEMA IF EXISTS {namespace.name} CASCADE ")
    namespace_obj.delete(namespace.id, is_cascade=False)
    return Message(message=f'The namespace:ID:{namespace.id} is removed')


namespace_router.include_router(table_router)
