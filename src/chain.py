import functools
import re

from src.provider import complete
from src.settings import DB_DIR

SYSTEM_PROMPT = """
You convert user questions to a single SAFE SQL SELECT for DuckDB. For Russian and English languages.
Rules:
- Output ONLY a SQL code block (```sql ... ```), no prose.
- SELECT only. FORBIDDEN: INSERT/UPDATE/DELETE/DDL/ATTACH/COPY.
- Always include explicit column list and LIMIT {row_limit} if not aggregating large sets.
- Use ISO timestamps; for year filters use BETWEEN y-01-01 AND (y+1)-01-01.
Schema:
{schema_docs}

Examples:
Q: top 5 countries by revenue in 2024
SQL:
SELECT c.country, round(SUM(o.total_amount), 2) AS revenue
FROM orders o JOIN customers c USING(customer_id)
WHERE o.order_ts >= '2024-01-01' AND o.order_ts < '2025-01-01'
GROUP BY 1
ORDER BY revenue DESC
LIMIT 5;
"""


@functools.lru_cache(maxsize=32)
def load_schema_docs() -> str:
    with open(DB_DIR / "schema_docs.md", "r", encoding="utf-8") as f:
        return f.read()


async def nl_to_sql(question: str, row_limit: int) -> str:
    system = SYSTEM_PROMPT.format(schema_docs=load_schema_docs(), row_limit=str(row_limit))
    user = f"Q: {question}\nSQL:\n"
    out = await complete(system, user)
    return out


async def refine(question: str, sql_md: str, feedback: str | None) -> str:
    """
    Simple refine strategy:
    - Add a short feedback to the user message about an error/empty result/unsafety
    - Ask to regenerate SQL while keeping safety constraints
    """
    # Minimal hint: strengthen the signal about LIMIT and filters
    hint = ""
    if feedback:
        hint = (f"\nConstraints: Fix issue -> {feedback}. Keep it a single safe SELECT for DuckDB. Prefer simpler "
                f"joins, ensure reasonable LIMIT.")
    # Ask the model again with the same limit (used inside nl_to_sql)
    improved_md = await nl_to_sql(question + hint, row_limit=100)
    return improved_md

def normalize_question(q: str) -> str:
    q = q.strip()
    q = re.sub(r"\s+", " ", q)
    # simple normalization of numbers/years
    q = q.replace("г.", "year").replace("года", "year")
    return q

def _extract_tokens(text: str) -> list[str]:
    return re.findall(r"[A-Za-zА-Яа-я0-9_]+", text.lower())


def similar_fields(q: str, schema_docs: str, topk: int = 5) -> list[str]:
    """
    Simplest "semantic" token matching:
    - Take question tokens
    - Find field/table description lines in schema_docs with the maximum token overlap
    """
    q_tokens = set(_extract_tokens(q))
    best: list[tuple[int, str]] = []
    for line in schema_docs.splitlines():
        tokens = set(_extract_tokens(line))
        if not tokens:
            continue
        score = len(q_tokens & tokens)
        if score > 0:
            best.append((score, line.strip()[:120]))
    best.sort(key=lambda x: (-x[0], x[1]))
    return [b[1] for b in best[:topk]]


async def make_plan(question: str, schema_docs: str | None = None) -> str:
    """
    Generates a brief plan for NL→SQL:
    - Goal/metric
    - Candidate tables/fields (by semantics)
    - Filters (including time)
    """
    qn = normalize_question(question)
    schema = schema_docs or load_schema_docs()
    fields = similar_fields(qn, schema, topk=5)
    bullets = [f"Goal: answer '{question}'"]
    if fields:
        bullets.append("Key fields/tables: " + ", ".join(fields))
    # simple heuristic about time
    if any(k in qn.lower() for k in ["год", "месяц", "quarter", "year", "month", "дата", "в 202", "за 202"]):
        bullets.append("Add a period filter, use ISO dates and BETWEEN y-01-01 AND (y+1)-01-01")
    bullets.append("Output: explicit list of columns, reasonable LIMIT")
    return " ; ".join(bullets)
