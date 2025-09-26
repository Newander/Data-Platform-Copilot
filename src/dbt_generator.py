import re
from pathlib import Path
from typing import Optional, Tuple

from src.chain import load_schema_docs, normalize_question
from src.io_utils import write_files_atomic
from src.provider import complete

SYSTEM_PROMPT_DBT = """
You are a senior Analytics Engineer who writes dbt models and tests.

Goal:
- Convert a business request into a single **dbt model SELECT** (no DML/DDL) and a minimal **schema.yml** with tests.
- Target engines are DuckDB/Postgres-compatible SQL.
- Model must be **idempotent** and **read-only**.

Strict output format:
- Output exactly two fenced code blocks:
  1) ```sql  -- model.sql
     SELECT ...
     ```
  2) ```yaml -- schema.yml
     version: 2
     models:
       - name: <model_name>
         description: <1-2 lines>
         columns:
           - name: <col>
             tests: [not_null]        # add unique if applicable
  - Do NOT add any prose between blocks.
  - Do NOT include ref() unless clearly needed (you may use source("...","...") if sources are evident).

Rules:
- SELECT only. FORBIDDEN: INSERT/UPDATE/DELETE/DDL/ATTACH/COPY.
- Prefer explicit column lists, stable aliases, and deterministic ordering when relevant.
- For year filters use BETWEEN y-01-01 AND (y+1)-01-01 (ISO timestamps).
- Add reasonable filters and joins based on provided schema docs.
- Make the model materialization-agnostic (no config{{}} block); the caller may set materialization in dbt.
- Use snake_case for columns and model names.

Schema docs:
{schema_docs}

Hints:
- Business request: "{question}"
- Suggested model_name: "{model_name}"
- If the request implies keys or natural primary keys (e.g., id), add 'unique' test for that column.
- Keep it compact and production-like.
"""


def _extract_block(text: str, lang: str) -> Optional[str]:
    """
    Extract the first fenced code block for a given language.
    ```lang
    ...
    ```
    """
    m = re.search(rf"```{lang}\s*(.*?)```", text, re.IGNORECASE | re.DOTALL)
    if not m:
        return None
    return m.group(1).strip()


def _sanitize_model_name(name: str) -> str:
    name = name.strip().lower()
    # replace non-word chars with underscore
    name = re.sub(r"[^a-z0-9_]+", "_", name)
    # collapse repeats, trim edges
    name = re.sub(r"_+", "_", name).strip("_")
    if not name:
        name = "generated_model"
    return name


async def generate_dbt_model(
        question: str,
        model_name: Optional[str] = None,
) -> Tuple[str, str, str]:
    """
    Returns (final_model_name, model_sql, schema_yml).
    - Uses schema_docs.md via load_schema_docs()
    - Enforces strict two-block output from LLM
    """
    qn = normalize_question(question)
    schema_docs = load_schema_docs()
    suggested_name = _sanitize_model_name(model_name or f"mart_{qn[:32]}")
    system = SYSTEM_PROMPT_DBT.format(
        schema_docs=schema_docs,
        question=qn,
        model_name=suggested_name,
    )
    user = "Produce the dbt model SELECT and schema.yml exactly as specified."
    out = await complete(system, user)
    sql_block = _extract_block(out, "sql")
    yaml_block = _extract_block(out, "yaml")

    if not sql_block or not yaml_block:
        # minimal fallback to keep API predictable
        raise RuntimeError("LLM did not return both sql and yaml blocks")

    # Optional: inject final model name into yaml if missing
    if f"name: {suggested_name}" not in yaml_block:
        yaml_block = re.sub(
            r"(models:\s*-\s*name:\s*)([a-zA-Z0-9_\-]+)",
            rf"\1{suggested_name}",
            yaml_block,
            count=1,
        )
        if f"name: {suggested_name}" not in yaml_block:
            # prepend a minimal yaml if regex failed
            yaml_block = f"""version: 2
models:
  - name: {suggested_name}
    description: Generated model
    columns: []
""" + "\n" + yaml_block

    return suggested_name, sql_block, yaml_block


def materialize_files_to_disk(
        dbt_root: Path,
        model_name: str,
        model_sql: str,
        schema_yml: str,
) -> dict[str, str]:
    files = {
        f"models/{model_name}.sql": model_sql,
        "models/schema.yml": schema_yml,
    }
    return write_files_atomic(dbt_root, files)
