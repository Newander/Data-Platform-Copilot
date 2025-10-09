import io
from typing import Annotated

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from pydantic import BaseModel, Field
from starlette import status

from src.database.base_model import depends_object
from src.database.db_connector import ConnectionType, opened_connection
from src.database.models import Table, NamespacePartModel, NamespaceFullModel, TableFullModel, TablePartModel
from src.route.inspect_schema import Message

table_router = APIRouter(prefix='/{namespace_id}/table')


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
        table_obj: Annotated[Table, Depends(depends_object(Table))],
) -> TableListResponse:
    tables = table_obj.all()
    return TableListResponse(
        message="OK" if tables else "No tables created",
        tables=tables
    )



class TableCreateModel(BaseModel):
    name: str


@table_router.post('/')
def create_table(
        namespace_id: int,
        table_obj: Annotated[Table, Depends(depends_object(Table))],
        new_table: TableCreateModel,
) -> TableFullModel:
    full_new_table = table_obj.insert(
        TablePartModel.model_validate(
            {'namespace_id': namespace_id,
             **new_table.model_dump()}
        )
    )
    return full_new_table


@table_router.post('/{table_id}/upload')
def upload_table_file(
        namespace: Annotated[NamespaceFullModel, Depends(get_namespace_depends)],
        table: Annotated[TableFullModel, Depends(get_table_depends)],
        connection: Annotated[ConnectionType, Depends(opened_connection)],
        file: UploadFile = File(...),
) -> TableFullModel:
    # todo: realise
    byte_data = file.file.read()

    table.file_name = file.filename
    table.file_size = len(byte_data)

    data_frame = pd.read_csv(io.BytesIO(byte_data))
    connection.execute(f"""
        CREATE OR REPLACE TABLE {namespace.name}.{table.name} AS 
        SELECT * FROM data_frame
    """)
    connection.commit()

    table.is_loaded = True
    return 'OK'


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
