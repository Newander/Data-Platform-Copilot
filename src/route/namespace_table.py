from typing import Annotated
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from starlette import status
from src.database.base_model import depends_object
from src.database.models import Namespace, NamespacePartModel, NamespaceFullModel

table_router = APIRouter(prefix='/{namespace_id}/table')


# Dependency to check namespace existence
async def get_namespace_depends(
        namespace_id: int,
        namespace_obj: Annotated[Namespace, Depends(depends_object(Namespace))]
) -> NamespaceFullModel:
    """Checks namespace existence and returns it, or raises 404"""
    namespace = namespace_obj.get(namespace_id)
    if not namespace:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Namespace with ID:{namespace_id} not found"
        )

    return namespace
