import duckdb
import re

from src.constants import ROW_LIMIT

SELECT_ONLY = re.compile(r"^\s*SELECT\b", re.IGNORECASE | re.DOTALL)
FORBIDDEN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|ATTACH|COPY|PRAGMA|EXPORT|IMPORT)\b",
    re.IGNORECASE
)


class IncorrectQuestionError(Exception):
    """ Returning on incorrect input question string """

def extract_sql_from_markdown(s: str) -> str:
    m = re.search(r"```sql(.*?)```", s, re.IGNORECASE | re.DOTALL)
    return m.group(1).strip() if m else s.strip()


def validate_sql(sql: str):
    if not SELECT_ONLY.match(sql):
        raise IncorrectQuestionError("Question asked for incorrect output")
    if FORBIDDEN.search(sql):
        raise IncorrectQuestionError("Statement contains forbidden keywords")
    # гарантируем LIMIT
    if "limit" not in sql.lower():
        sql += f"\nLIMIT {ROW_LIMIT}"
    return sql


def run(outer_sql: str):
    sql = validate_sql(outer_sql)
    con = duckdb.connect("data/demo.duckdb")
    con.execute(f"SET threads TO 2; SET memory_limit='512MB';")
    plan = con.execute("EXPLAIN " + sql).fetchdf()
    df = con.execute(sql).fetchdf()
    con.close()
    # превью
    preview = df.head(min(len(df), 20))
    return plan.to_string().strip(), preview
