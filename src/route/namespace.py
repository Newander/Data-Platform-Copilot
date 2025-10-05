from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from starlette import status

from src.database.base_model import depends_object
from src.database.models import Namespace, NamespacePartModel, NamespaceFullModel

namespace_router = APIRouter(prefix='/namespace')


class Message(BaseModel):
    message: str


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
        namespace_obj: Annotated[Namespace, Depends(depends_object(Namespace))],
        new_namespace: NamespacePartModel,
) -> NamespaceFullModel:
    full_new_namespace = namespace_obj.insert(new_namespace)
    return full_new_namespace


@namespace_router.get('/{namespace_id}')
def get_namespace(
        namespace_obj: Annotated[Namespace, Depends(depends_object(Namespace))],
        namespace_id: int,
) -> NamespaceFullModel:
    if namespace := namespace_obj.get(namespace_id):
        return namespace
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Namespace with ID:{namespace_id} not found")


@namespace_router.put('/{namespace_id}')
def edit_namespace(
        namespace_obj: Annotated[Namespace, Depends(depends_object(Namespace))],
        namespace_id: int,
        updated_namespace: NamespacePartModel,
) -> NamespaceFullModel | Message:
    if not (namespace := namespace_obj.get(namespace_id)):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Namespace with ID:{namespace_id} not found")

    if namespace == updated_namespace:
        return namespace

    return namespace_obj.update(
        NamespaceFullModel(id=namespace_id, **updated_namespace.model_dump())
    )


@namespace_router.delete('/{namespace_id}')
def delete_namespace(
        namespace_obj: Annotated[Namespace, Depends(depends_object(Namespace))],
        namespace_id: int,
) -> Message:
    # todo: remove also tables
    namespace_obj.delete(namespace_id)
    return Message(message=f'The namespace:ID:{namespace_id} is removed')
