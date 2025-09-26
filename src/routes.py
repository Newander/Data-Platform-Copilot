import time

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from src.chain import nl_to_sql, make_plan, refine
from src.dbt_generator import generate_dbt_model
from src.metrics import METRICS
from src.settings import ROW_LIMIT
from src.sql_runner import extract_sql_from_markdown, sql_run, IncorrectQuestionError, is_safe

common_router = APIRouter()


class AskRequest(BaseModel):
    question: str


class AskResponse(BaseModel):
    answer: str | None = None


class ChatIn(BaseModel):
    question: str


class ChatOut(BaseModel):
    sql: str
    plan: str
    rows: list


@common_router.post("/chat", response_model=ChatOut)
async def chat(inp: ChatIn):
    sql_md = await nl_to_sql(inp.question, ROW_LIMIT)
    if not sql_md:
        raise HTTPException(500, "LLM provider not configured")

    sql = extract_sql_from_markdown(sql_md)
    try:
        plan, preview = sql_run(sql)
    except IncorrectQuestionError as err:
        raise HTTPException(400, err.args[0]) from err

    return ChatOut(sql=sql, plan=plan, rows=preview.to_dict(orient="records"))


class AgentIn(BaseModel):
    question: str
    max_steps: int | None = 2


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


@common_router.post("/chat/agent", response_model=AgentOut)
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
            plan_text, preview = sql_run(sql)
            exec_ms = int((time.perf_counter() - t3) * 1000)
            exec_ms_acc += exec_ms
            recs = preview.to_dict(orient="records")
            candidates.append(CandidateSQL(sql=sql, reason=f"ok:{len(recs)}rows, {exec_ms}ms"))
            # selection heuristic: first successful with non-empty rows
            if recs and not chosen_sql:
                chosen_sql = sql
                rows = recs
                explain = f"Query follows the plan: {plan}. Tables and filters match the description. "
                break
            # if empty — refine and try again
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

    # if we didn't get a non-empty success — take the last successful (if any) or the last candidate
    if not chosen_sql:
        # Try to return the last valid run even if empty
        for c in reversed(candidates):
            if c.reason.startswith("ok"):
                chosen_sql = c.sql
                try:
                    _, preview = sql_run(chosen_sql)
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
        explain=explain or f"Query generated according to the plan. Last status: {candidates[-1].reason if candidates else 'n/a'}.",
        telemetry=telemetry,
    )


class DbtGenIn(BaseModel):
    question: str = Field(..., description="Business question to convert into a dbt model")
    model_name: str | None = Field(None, description="Optional target model name (snake_case)")


class DbtGenOut(BaseModel):
    model_name: str
    files: dict  # {"models/<model_name>.sql": "...", "models/schema.yml": "..."}


@common_router.post("/dbt/generate", response_model=DbtGenOut)
async def dbt_generate(inp: DbtGenIn):
    # Optionally, you can add a quick plan preview (not required for generation)
    # plan = await make_plan(inp.question)  # not returned but could be logged/observed

    model_name, model_sql, schema_yml = await generate_dbt_model(
        question=inp.question,
        model_name=inp.model_name,
    )

    # We just return file contents; writing to disk/PR
    files = {
        f"models/{model_name}.sql": model_sql,
        "models/schema.yml": schema_yml,
    }
    return DbtGenOut(model_name=model_name, files=files)
