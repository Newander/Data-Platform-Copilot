import logging
import time
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from src.chain import nl_to_sql, refine, make_plan
from src.constants import ROW_LIMIT, LOG_LEVEL, LOG_FORMAT, DATE_FORMAT, HOST, PORT
from src.metrics import METRICS
from src.sql_runner import extract_sql_from_markdown, run, IncorrectQuestionError, is_safe

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


class AgentIn(BaseModel):
    question: str
    max_steps: Optional[int] = 2


class CandidateSQL(BaseModel):
    sql: str
    reason: str


class AgentOut(BaseModel):
    plan: str
    candidates: list[CandidateSQL]
    chosen_sql: str
    rows: list
    explain: str
    telemetry: dict


@app.post("/chat/agent", response_model=AgentOut)
async def chat_agent(inp: AgentIn):
    METRICS.inc("ai_requests_total", {"route": "agent"})
    plan = await make_plan(inp.question)
    candidates: list[CandidateSQL] = []
    chosen_sql = ""
    explain = ""
    rows = []
    gen_ms_acc = 0
    exec_ms_acc = 0
    retries = 0

    # First try generation
    t0 = time.perf_counter()
    draft_md = await nl_to_sql(inp.question, ROW_LIMIT)
    gen_ms_acc += int((time.perf_counter() - t0) * 1000)
    if not draft_md:
        METRICS.inc("ai_errors_total", {"stage": "generate"})
        raise HTTPException(500, "LLM provider not configured")

    # Extract SQL and check safety
    sql = extract_sql_from_markdown(draft_md)
    ok, reason = is_safe(sql)
    if not ok:
        candidates.append(CandidateSQL(sql=sql, reason=f"blocked: {reason}"))
        # Refine with feedback
        retries += 1
        t1 = time.perf_counter()
        draft_md = await refine(inp.question, draft_md, f"unsafe: {reason}")
        gen_ms_acc += int((time.perf_counter() - t1) * 1000)
        sql = extract_sql_from_markdown(draft_md)

    # Execute refine with emptiness
    step = 0
    last_error = None
    while step < (inp.max_steps or 2):
        step += 1
        ok, reason = is_safe(sql)
        if not ok:
            candidates.append(CandidateSQL(sql=sql, reason=f"blocked: {reason}"))
            retries += 1
            t2 = time.perf_counter()
            draft_md = await refine(inp.question, draft_md, f"unsafe: {reason}")
            gen_ms_acc += int((time.perf_counter() - t2) * 1000)
            sql = extract_sql_from_markdown(draft_md)
            continue

        try:
            t3 = time.perf_counter()
            plan_text, preview = run(sql)
            exec_ms = int((time.perf_counter() - t3) * 1000)
            exec_ms_acc += exec_ms
            recs = preview.to_dict(orient="records")
            candidates.append(CandidateSQL(sql=sql, reason=f"ok:{len(recs)}rows, {exec_ms}ms"))
            # эвристика выбора: первая успешная с непустыми строками
            if recs and not chosen_sql:
                chosen_sql = sql
                rows = recs
                explain = f"Запрос следует плану: {plan}. Таблицы и фильтры соответствуют описанию. "
                break

            # если пусто — рефайн и еще попытка
            if not recs:
                last_error = "empty"
                retries += 1
                t4 = time.perf_counter()
                draft_md = await refine(inp.question, draft_md,
                                        "empty result, add broader filters or remove overly strict predicates")
                gen_ms_acc += int((time.perf_counter() - t4) * 1000)
                sql = extract_sql_from_markdown(draft_md)
                continue

        except IncorrectQuestionError as err:
            last_error = str(err)
            candidates.append(CandidateSQL(sql=sql, reason=f"error:{last_error}"))
            METRICS.inc("ai_errors_total", {"stage": "execute"})
            retries += 1
            t5 = time.perf_counter()
            draft_md = await refine(inp.question, draft_md, f"execution error: {last_error}")
            gen_ms_acc += int((time.perf_counter() - t5) * 1000)
            sql = extract_sql_from_markdown(draft_md)

    # если так и не получили непустой успех — берем последний успешный (если был) или последний кандидат
    if not chosen_sql:
        # Пытаемся вернуть последний валидный запуск даже если пустой
        for c in reversed(candidates):
            if c.reason.startswith("ok"):
                chosen_sql = c.sql
                try:
                    _, preview = run(chosen_sql)
                    rows = preview.to_dict(orient="records")
                except Exception:
                    rows = []
                break
        if not chosen_sql and candidates:
            chosen_sql = candidates[-1].sql

    telemetry = {"gen_ms": gen_ms_acc, "exec_ms": exec_ms_acc, "retries": retries, "last_error": last_error}
    METRICS.observe_ms("ai_sql_generation_ms", gen_ms_acc, {})
    METRICS.observe_ms("ai_sql_exec_ms", exec_ms_acc, {})
    if last_error == "empty":
        METRICS.inc("ai_sql_empty_results_total", {})

    return AgentOut(
        plan=plan,
        candidates=candidates,
        chosen_sql=chosen_sql,
        rows=rows,
        explain=explain or f"Сформирован запрос по плану. Последний статус: {candidates[-1].reason if candidates else 'n/a'}.",
        telemetry=telemetry,
    )


@app.get("/metrics")
def metrics():
    """ Prometheus related """
    return METRICS.export_prometheus()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host=HOST, port=PORT, reload=True)
