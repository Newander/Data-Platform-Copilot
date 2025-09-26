import functools

from src.constants import DB_DIR
from src.provider import complete

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
    with open(f"{DB_DIR}/schema_docs.md", "r", encoding="utf-8") as f:
        return f.read()


async def nl_to_sql(question: str, row_limit: int) -> str:
    system = SYSTEM_PROMPT.format(schema_docs=load_schema_docs(), row_limit=str(row_limit))
    user = f"Q: {question}\nSQL:\n"
    out = await complete(system, user)
    return out
