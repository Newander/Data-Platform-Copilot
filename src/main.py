import logging
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from src.chain import nl_to_sql
from src.constants import ROW_LIMIT, LOG_LEVEL, LOG_FORMAT, DATE_FORMAT, HOST, PORT
from src.sql_runner import extract_sql_from_markdown, run, IncorrectQuestionError

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
    sql_md = await nl_to_sql(inp.question, ROW_LIMIT)
    if not sql_md:
        raise HTTPException(500, "LLM provider not configured")

    sql = extract_sql_from_markdown(sql_md)
    try:
        plan, preview = run(sql)
    except IncorrectQuestionError as err:
        raise HTTPException(400, err.args[0]) from err
    return ChatOut(sql=sql, plan=plan, rows=preview.to_dict(orient="records"))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host=HOST, port=PORT, reload=True)
