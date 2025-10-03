import re

import duckdb

from src.config import ROW_LIMIT, DATA_DIR, DB_FILE_NAME

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
    # guaranteed LIMIT
    if "limit" not in sql.lower():
        sql += f"\nLIMIT {ROW_LIMIT}"
    return sql


def is_safe(sql: str) -> tuple[bool, str]:
    if not sql or not isinstance(sql, str):
        return False, "empty"
    # remove backticks and extra
    body = sql.strip().strip("`")
    if FORBIDDEN.search(body):
        return False, "forbidden keyword"
    if not SELECT_ONLY.search(body):
        return False, "only SELECT allowed"
    # limit on the number of queries — disallow multiple statements
    if ";" in body.strip().rstrip(";"):
        return False, "multiple statements"
    # restriction on comments to prevent hidden DDL from slipping through
    if re.search(r"/\*.*\*/", body, re.DOTALL):
        return False, "block comments not allowed"
    # basic heuristic for LIMIT (if no aggregates/explicitly small sets)
    # soft warning do not block: advice only
    return True, "ok"


def sql_run(outer_sql: str):
    sql = validate_sql(outer_sql)
    con = duckdb.connect(DATA_DIR / settings.database.file_name)
    con.execute(f"SET threads TO 2; SET memory_limit='512MB';")
    plan = con.execute("EXPLAIN " + sql).fetchdf()
    df = con.execute(sql).fetchdf()
    con.close()
    # preview
    preview = df.head(min(len(df), 20))
    return plan.to_string().strip(), preview
