from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from src.config import DemoDataPath
from src.database.base_model import depends_object
from src.database.db_connector import ConnectionType, opened_connection
from src.database.models import Namespace, NamespaceNameModel, NamespaceFullModel, NamespaceCreateModel, Table, \
    TableFullModel
from src.route.inspect_schema import Message
from src.route.namespace_table import table_router, get_namespace_depends
from src.utils import normalize_schema_name

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
        new_namespace: NamespaceNameModel,
) -> NamespaceFullModel:
    schema_name = normalize_schema_name(new_namespace.name)
    connection.execute(f" CREATE SCHEMA IF NOT EXISTS {schema_name} ")
    full_new_namespace = namespace_obj.insert(
        NamespaceCreateModel.model_validate({
            'schema_name': schema_name,
            **new_namespace.model_dump(),
        })
    )
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
        updated_namespace: NamespaceNameModel,
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



class DemoUploadResponse(BaseModel):
    message: str
    namespace: NamespaceFullModel
    tables: list[TableFullModel]
    files_processed: int

@namespace_router.post(
    "/demo-upload",
    description='Upload demo models to the database from the /demo_data folder (replace)',
)
async def upload_demo_models(
        connection: Annotated[ConnectionType, Depends(opened_connection)],
        namespace_obj: Annotated[Namespace, Depends(depends_object(Namespace))],
        table_obj: Annotated[Table, Depends(depends_object(Table))],
) -> DemoUploadResponse:
    if not DemoDataPath.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Demo data directory not found: {DemoDataPath}"
        )

    if not (csv_files := list(DemoDataPath.glob("*.csv"))):
        raise HTTPException(
            status_code=404,
            detail=f"No CSV files found in {DemoDataPath}"
        )

    namespace_name = "Demo Dataset"
    schema_name = normalize_schema_name(namespace_name)

    if demo_schemas := namespace_obj.filter(schema_name=schema_name):
        demo_schema = demo_schemas[0]
    else:
        ...

    # Create new demo namespace
    connection.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    connection.commit()

    namespace = namespace_obj.insert(
        NamespaceCreateModel(
            name=namespace_name,
            schema_name=schema_name
        )
    )

    created_tables = []
    files_processed = 0

    # Обрабатываем каждый CSV файл
    for csv_file in csv_files:
        table_name = csv_file.stem  # имя файла без расширения

        # Читаем CSV файл
        try:
            data_frame = pd.read_csv(csv_file)
        except Exception as e:
            raise HTTPException(
                status_code=400,
                detail=f"Error reading CSV file {csv_file.name}: {str(e)}"
            )

        if data_frame.empty:
            continue

        # Создаем таблицу в базе данных
        try:
            connection.execute(f"""
                CREATE OR REPLACE TABLE {schema_name}.{table_name} AS 
                SELECT * FROM data_frame
            """)
            connection.commit()
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Error creating table {table_name}: {str(e)}"
            )

        # Записываем информацию о таблице в метаданные
        table_record = table_obj.insert(
            TableCreateModel(
                namespace_id=namespace.id,
                table_name=table_name,
                file_name=csv_file.name,
                file_size=csv_file.stat().st_size,
                is_loaded=True
            )
        )

        created_tables.append(table_record)
        files_processed += 1

    if files_processed == 0:
        raise HTTPException(
            status_code=400,
            detail="No valid CSV files were processed"
        )

    return DemoUploadResponse(
        message=f"Successfully uploaded {files_processed} demo tables to namespace '{namespace_name}'",
        namespace=namespace,
        tables=created_tables,
        files_processed=files_processed
    )


namespace_router.include_router(table_router)
