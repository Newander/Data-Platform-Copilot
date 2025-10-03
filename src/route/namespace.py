from fastapi import APIRouter, Depends
from pydantic import BaseModel

from src.database.db_connector import opened_connection
from src.config import settings

namespace_router = APIRouter(prefix='/namespace')


class Namespace(BaseModel):
    name: str


class NamespaceListResponse(BaseModel):
    message: str
    namespaces: list[Namespace]


@namespace_router.get('/')
def list_namespaces(connection=Depends(opened_connection)) -> NamespaceListResponse:
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
