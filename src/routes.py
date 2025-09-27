import re
import time
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from src.chain import nl_to_sql, make_plan, refine
from src.dbt_generator import generate_dbt_model, materialize_files_to_disk
from src.dq import run_checks, render_markdown_report, fetch_table_sample, profile_df
from src.github_client import create_branch, upsert_file, create_pull_request, GitHubError
from src.demo_seed import seed_events
from src.metrics import METRICS
from src.settings import DQ_DEFAULT_LIMIT
from src.settings import ROW_LIMIT, DBT_DIR, GIT_DEFAULT_BRANCH
from src.sql_runner import extract_sql_from_markdown, sql_run, IncorrectQuestionError, is_safe, validate_sql
from src.schema_docs import write_schema_docs
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
    model_name: Optional[str] = Field(None, description="Optional target model name (snake_case)")
    write: bool = Field(False, description="If true, write files to DBT_DIR")


class DbtGenOut(BaseModel):
    model_name: str
    files: dict  # {"models/<model>.sql": "...", "models/schema.yml": "..."}
    written_paths: Optional[dict[str, str]] = None


@common_router.post("/dbt/generate", response_model=DbtGenOut)
async def dbt_generate(inp: DbtGenIn):
    model_name, model_sql, schema_yml = await generate_dbt_model(
        question=inp.question,
        model_name=inp.model_name,
    )
    files = {
        f"models/{model_name}.sql": model_sql,
        "models/schema.yml": schema_yml,
    }
    written_paths = None
    if inp.write:
        written_paths = materialize_files_to_disk(DBT_DIR, model_name, model_sql, schema_yml)
    return DbtGenOut(model_name=model_name, files=files, written_paths=written_paths)


# === dbt preview: сухой прогон SELECT ===
class DbtPreviewIn(BaseModel):
    model_sql: str = Field(..., description="SELECT statement for preview (DuckDB-compatible)")
    limit_override: Optional[int] = Field(None, description="Optional limit override")


class DbtPreviewOut(BaseModel):
    plan: str
    rows: list


@common_router.post("/dbt/preview", response_model=DbtPreviewOut)
async def dbt_preview(inp: DbtPreviewIn):
    sql = inp.model_sql
    # валидация и гарантия LIMIT:
    sql_validated = validate_sql(sql)
    if inp.limit_override and inp.limit_override > 0:
        # грубая замена последнего LIMIT — для простоты оставим базовую реализацию
        sql_validated = re.sub(r"(?i)\blimit\s+\d+\s*$", f"LIMIT {inp.limit_override}",
                               sql_validated.strip())  # type: ignore
    plan, preview = sql_run(sql_validated)
    return DbtPreviewOut(plan=plan, rows=preview.to_dict(orient="records"))


# === dbt PR в GitHub ===
class DbtPROut(BaseModel):
    branch: str
    files_committed: dict[str, str]
    pr_url: str


class DbtPRIn(BaseModel):
    title: str = Field(..., description="PR title")
    branch: str = Field(..., description="Feature branch to create or reuse")
    base: Optional[str] = Field(None, description="Base branch (default from settings)")
    files: dict[str, str] = Field(..., description="Repo-relative paths → contents (e.g., models/x.sql)")


@common_router.post("/dbt/pr", response_model=DbtPROut)
async def dbt_pr(inp: DbtPRIn):
    try:
        # создаём/проверяем ветку
        await create_branch(inp.branch, from_branch=inp.base or GIT_DEFAULT_BRANCH)
        committed: dict[str, str] = {}
        for path, body in inp.files.items():
            r = await upsert_file(
                path=path,
                content=body,
                branch=inp.branch,
                message=f"chore(dbt): add/update {path}",
            )
            committed[path] = r.get("content", {}).get("sha", "ok")
        pr = await create_pull_request(
            title=inp.title,
            head=inp.branch,
            base=inp.base or GIT_DEFAULT_BRANCH,
            body="Automated PR from Data Platform Copilot",
        )
        return DbtPROut(branch=inp.branch, files_committed=committed, pr_url=pr.get("html_url", ""))
    except GitHubError as e:
        raise HTTPException(status_code=400, detail=str(e))


class DQProfileIn(BaseModel):
    table: str
    where: str | None = None
    limit: int | None = None


class DQProfileOut(BaseModel):
    profile: dict
    sample_rows: list


@common_router.post("/dq/profile", response_model=DQProfileOut)
async def dq_profile(inp: DQProfileIn):
    METRICS.inc("dq_requests_total", {"route": "profile"})
    df = fetch_table_sample(inp.table, where=inp.where, limit=inp.limit or DQ_DEFAULT_LIMIT)
    prof = profile_df(df)
    return DQProfileOut(
        profile=prof,
        sample_rows=df.head(min(len(df), 20)).to_dict(orient="records"),
    )


class DQRule(BaseModel):
    type: str  # not_null|unique|range|freshness|anomaly
    column: str | None = None
    min: float | None = None
    max: float | None = None
    max_age_hours: int | None = None
    sigma: float | None = None


class DQCheckIn(BaseModel):
    table: str
    where: str | None = None
    rules: list[DQRule]
    sample_limit: int | None = None


class DQCheckOut(BaseModel):
    passed: bool
    results: list[dict]
    markdown_report: str
    sample_rows: list


@common_router.post("/dq/check", response_model=DQCheckOut)
async def dq_check(inp: DQCheckIn):
    METRICS.inc("dq_requests_total", {"route": "check"})
    prof, results, sample = run_checks(
        table=inp.table,
        where=inp.where,
        rules=[r.model_dump() for r in inp.rules],
        sample_limit=inp.sample_limit or DQ_DEFAULT_LIMIT,
    )
    md = render_markdown_report(inp.table, inp.where, prof, results)
    passed = all(r.passed for r in results)
    # Телеметрия
    METRICS.inc("dq_checks_total", {"passed": "true" if passed else "false"})
    return DQCheckOut(
        passed=passed,
        results=[{"rule": r.rule, "passed": r.passed, "details": r.details} for r in results],
        markdown_report=md,
        sample_rows=sample.to_dict(orient="records"),
    )


class DemoSeedIn(BaseModel):
    rows: int | None = 100_000


class DemoSeedOut(BaseModel):
    table: str
    rows: int
    min_ts: str
    max_ts: str
    schema_docs_path: str


@common_router.post("/demo/seed/events", response_model=DemoSeedOut)
async def demo_seed_events(inp: DemoSeedIn):
    stats = seed_events(n_rows=inp.rows or 100_000)
    # обновим schema_docs.md и сбросим кэш промпта
    path = write_schema_docs()
    try:
        load_schema_docs.cache_clear()  # type: ignore[attr-defined]
    except Exception:
        pass
    return DemoSeedOut(
        table=stats["table"],
        rows=stats["rows"],
        min_ts=stats["min_ts"],
        max_ts=stats["max_ts"],
        schema_docs_path=path,
    )


class SchemaRefreshOut(BaseModel):
    schema_docs_path: str
    size_bytes: int


@common_router.post("/schema/refresh", response_model=SchemaRefreshOut)
async def schema_refresh():
    path = write_schema_docs()
    try:
        load_schema_docs.cache_clear()  # type: ignore[attr-defined]
    except Exception:
        pass
    p = Path(path)
    return SchemaRefreshOut(schema_docs_path=path, size_bytes=p.stat().st_size if p.exists() else 0)
