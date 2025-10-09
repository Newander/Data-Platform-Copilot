import io
from typing import Annotated

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from pydantic import BaseModel, Field
from starlette import status

from src.database.base_model import depends_object
from src.database.db_connector import ConnectionType, opened_connection
from src.database.models import Table, NamespaceNameModel, NamespaceFullModel, TableFullModel, TablePartModel, Namespace
from src.route.inspect_schema import Message
from src.utils import normalize_schema_name, validate_csv_file

table_router = APIRouter(prefix='/{namespace_id}/table')


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


async def get_table_depends(
        table_id: int,
        namespace_id: int,
        table_obj: Annotated[Table, Depends(depends_object(Table))],
) -> TableFullModel:
    """Checks namespace existence and returns it, or raises 404"""
    table = table_obj.get(table_id)
    if not table or table.namespace_id != namespace_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Table with ID:{table_id} not found"
        )

    return table


class TableListResponse(BaseModel):
    message: str
    tables: list[TableFullModel]


@table_router.get('/')
def list_tables(
        namespace: Annotated[NamespaceFullModel, Depends(get_namespace_depends)],
        table_obj: Annotated[Table, Depends(depends_object(Table))],
) -> TableListResponse:
    tables = [
        table for table in table_obj.all() if table.namespace_id == namespace.id
    ]
    return TableListResponse(
        message="OK" if tables else "No tables created",
        tables=tables
    )


class TableCreateModel(BaseModel):
    name: str


@table_router.post('/')
def create_table(
        namespace: Annotated[NamespaceFullModel, Depends(get_namespace_depends)],
        table_obj: Annotated[Table, Depends(depends_object(Table))],
        new_table: TableCreateModel,
) -> TableFullModel:
    full_new_table = table_obj.insert(
        TablePartModel.model_validate(
            {'namespace_id': namespace.id,
             'table_name': normalize_schema_name(new_table.name),
             **new_table.model_dump()}
        )
    )
    return full_new_table


@table_router.post('/{table_id}/upload')
async def upload_table_file(
        namespace: Annotated[NamespaceFullModel, Depends(get_namespace_depends)],
        table: Annotated[TableFullModel, Depends(get_table_depends)],
        table_obj: Annotated[Table, Depends(depends_object(Table))],
        connection: Annotated[ConnectionType, Depends(opened_connection)],
        file: Annotated[UploadFile, Depends(validate_csv_file)],
) -> TableFullModel:
    if table.is_loaded:
        raise HTTPException(status_code=400, detail="Table is already loaded")

    byte_data = file.file.read()

    table.file_name = file.filename
    table.file_size = len(byte_data)

    data_frame = pd.read_csv(io.BytesIO(byte_data))
    connection.execute(f"""
        CREATE OR REPLACE TABLE {namespace.schema_name}.{table.table_name} AS 
        SELECT * FROM data_frame
    """)
    connection.commit()

    table.is_loaded = True
    table_obj.update(table)

    return table


@table_router.get('/{table_id}')
def get_table(
        namespace: Annotated[NamespaceFullModel, Depends(get_namespace_depends)],
) -> NamespaceFullModel:
    return namespace


@table_router.put('/{table_id}')
def edit_table(
        table_obj: Annotated[Table, Depends(depends_object(Table))],
        exist_table: Annotated[NamespaceFullModel, Depends(get_namespace_depends)],
        updated_table: NamespaceNameModel,
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
    table_obj.delete(namespace.id)
    return Message(message=f'The namespace:ID:{namespace.id} is removed')
