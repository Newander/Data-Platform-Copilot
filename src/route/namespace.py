from typing import Annotated

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from src.database.db_connector import ConnectionCM

namespace_router = APIRouter(prefix='/namespace')


class Namespace(BaseModel):
    name: str


@namespace_router.get('/')
def list_namespaces(
        connection_manager: Annotated[ConnectionCM, Depends(ConnectionCM)],
) -> list[Namespace]:
    with connection_manager as connection:
        schema_names = connection.execute(
            """
            select distinct schemata.schema_name
            from information_schema.schemata
            where schemata.schema_name != 'main'
            order by 1
            """
        ).fetchall()

    return [Namespace(name=schema_name) for schema_name in schema_names]
