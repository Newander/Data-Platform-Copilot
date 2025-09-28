from typing import Annotated

from sqlalchemy import MetaData, Column, Integer, String
from sqlalchemy.orm import declarative_base

from src.settings import settings

CommonMetadata = MetaData(schema=settings.database.default_schema)
DeclarativeBase = declarative_base(metadata=CommonMetadata)


class BaseSQLModel(DeclarativeBase):
    __abstract__ = True


class Namespace(BaseSQLModel):
    __tablename__ = "namespace"

    id: Annotated[int, Column(Integer, primary_key=True, autoincrement=True)]
    name: Annotated[str, Column(String(1024), nullable=False)]
