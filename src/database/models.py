from sqlalchemy import MetaData, Integer, String
from sqlalchemy.orm import declarative_base, Mapped, mapped_column

from src.settings import settings

CommonMetadata = MetaData(schema=settings.database.default_schema)
DeclarativeBase = declarative_base(metadata=CommonMetadata)


class BaseSQLModel(DeclarativeBase):
    __abstract__ = True


class Namespace(BaseSQLModel):
    __tablename__ = "namespace"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(1024), nullable=False)
