from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from starlette import status

from src.database.base_model import depends_object
from src.database.models import Table, NamespacePartModel, NamespaceFullModel, TableFullModel
from src.route.inspect_schema import Message

table_router = APIRouter(prefix='/{namespace_id}/table')


# Dependency to check namespace existence
async def get_namespace_depends(
        namespace_id: int,
        namespace_obj: Annotated[Table, Depends(depends_object(Table))]
) -> NamespaceFullModel:
    """Checks namespace existence and returns it, or raises 404"""
    namespace = namespace_obj.get(namespace_id)
    if not namespace:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Table with ID:{namespace_id} not found"
        )

    return namespace


class TableListResponse(BaseModel):
    message: str
    tables: list[TableFullModel]


class TableResponse(BaseModel):
    message: str
    namespace: TableFullModel | None


@table_router.get('/')
def list_tables(
        table_obj: Annotated[Table, Depends(depends_object(Table))],
) -> TableListResponse:
    tables = table_obj.all()
    return TableListResponse(
        message="OK" if tables else "No tables created",
        tables=tables
    )


@table_router.post('/')
def create_table(
        table_obj: Annotated[Table, Depends(depends_object(Table))],
        new_table: NamespacePartModel,
) -> NamespaceFullModel:
    full_new_table = table_obj.insert(new_table)
    return full_new_table


@table_router.get('/{table_id}')
def get_table(
        namespace: Annotated[NamespaceFullModel, Depends(get_namespace_depends)],
) -> NamespaceFullModel:
    return namespace


@table_router.put('/{table_id}')
def edit_table(
        table_obj: Annotated[Table, Depends(depends_object(Table))],
        exist_table: Annotated[NamespaceFullModel, Depends(get_namespace_depends)],
        updated_table: NamespacePartModel,
) -> NamespaceFullModel:
    if exist_table == updated_table:
        return exist_table

    return table_obj.update(
        NamespaceFullModel(id=exist_table.id, **updated_table.model_dump())
    )


@table_router.delete('/{table_id}')
def delete_table(
        table_obj: Annotated[Table, Depends(depends_object(Table))],
        namespace: Annotated[NamespaceFullModel, Depends(get_namespace_depends)],
) -> Message:
    # todo: remove also tables
    table_obj.delete(namespace.id)
    return Message(message=f'The namespace:ID:{namespace.id} is removed')
