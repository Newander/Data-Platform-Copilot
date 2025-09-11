import logging
import os
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from src.chain import nl_to_sql
from src.sql_runner import extract_sql_from_markdown, run

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_FORMAT = os.getenv("LOG_FORMAT", "%(asctime)s | %(levelname)s | %(name)s | %(message)s")
DATE_FORMAT = os.getenv("LOG_DATEFMT", "%Y-%m-%d %H:%M:%S")

logging.basicConfig(
    level=LOG_LEVEL,
    format=LOG_FORMAT,
    datefmt=DATE_FORMAT,
    force=True,  # перезаписывает существующую конфигурацию логирования (полезно при повторных запусках)
)
app = FastAPI(
    title="Simple FastAPI with SQLAlchemy",
    debug=True
)


class AskRequest(BaseModel):
    question: str


class AskResponse(BaseModel):
    answer: Optional[str] = None


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


class ChatIn(BaseModel):
    question: str


class ChatOut(BaseModel):
    sql: str
    plan: str
    rows: list


@app.post("/chat", response_model=ChatOut)
async def chat(inp: ChatIn):
    sql_md = await nl_to_sql(inp.question, int(os.getenv("ROW_LIMIT", "200")))
    if not sql_md:
        raise HTTPException(500, "LLM provider not configured")

    sql = extract_sql_from_markdown(sql_md)
    plan, preview = run(sql)
    return ChatOut(sql=sql, plan=plan, rows=preview.to_dict(orient="records"))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host=os.getenv("HOST", "0.0.0.0"), port=int(os.getenv("PORT", "8000")), reload=True)
