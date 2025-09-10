import os
from typing import Generator, Optional

from fastapi import Depends, FastAPI, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session, declarative_base


# DB environment variables
def build_db_url() -> str:
    db_url = os.getenv("DB_URL")
    if db_url:
        return db_url

    # if DB_URL is not correct, try to set default
    db_driver = os.getenv("DB_DRIVER", "postgresql+psycopg2")
    db_user = os.getenv("DB_USER", "postgres")
    db_password = os.getenv("DB_PASSWORD", "postgres")
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "app_db")
    return f"{db_driver}://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"


DATABASE_URL = build_db_url()

engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
Base = declarative_base()


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


app = FastAPI(title="Simple FastAPI with SQLAlchemy")


class AskRequest(BaseModel):
    question: str


class AskResponse(BaseModel):
    answer: Optional[str] = None


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/ask", response_model=AskResponse)
def ask(_: AskRequest, db: Session = Depends(get_db)) -> JSONResponse:
    return JSONResponse(status_code=status.HTTP_200_OK, content=AskResponse(answer=None).model_dump())


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host=os.getenv("HOST", "0.0.0.0"), port=int(os.getenv("PORT", "8000")), reload=True)
