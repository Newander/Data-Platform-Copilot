from src.provider import complete
from src.schema_introspect import load_schema_docs

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
SELECT c.country, SUM(o.total_amount) AS revenue
FROM orders o JOIN customers c USING(customer_id)
WHERE o.order_ts >= '2024-01-01' AND o.order_ts < '2025-01-01'
GROUP BY 1
ORDER BY revenue DESC
LIMIT 5;
"""


async def nl_to_sql(question: str, row_limit: int) -> str:
    prompt = SYSTEM_PROMPT.format(schema_docs=load_schema_docs(), row_limit=row_limit)
    out = await complete(prompt + f"\nQ: {question}\nSQL:\n")
    return out
