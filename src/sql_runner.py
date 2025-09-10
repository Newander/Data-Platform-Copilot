import duckdb, re, os, pandas as pd

ROW_LIMIT = int(os.getenv("ROW_LIMIT", "200"))
TIMEOUT_MS = int(os.getenv("QUERY_TIMEOUT_MS", "8000"))

SELECT_ONLY = re.compile(r"^\s*SELECT\b", re.IGNORECASE | re.DOTALL)
FORBIDDEN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|ATTACH|COPY|PRAGMA|EXPORT|IMPORT)\b",
    re.IGNORECASE
)


def extract_sql_from_markdown(s: str) -> str:
    m = re.search(r"```sql(.*?)```", s, re.IGNORECASE | re.DOTALL)
    return m.group(1).strip() if m else s.strip()


def validate_sql(sql: str):
    if not SELECT_ONLY.match(sql):
        raise ValueError("Only SELECT statements are allowed.")
    if FORBIDDEN.search(sql):
        raise ValueError("Statement contains forbidden keywords.")
    # гарантируем LIMIT
    if " limit " not in sql.lower():
        sql += f"\nLIMIT {ROW_LIMIT}"
    return sql


def run(sql: str):
    sql = validate_sql(sql)
    con = duckdb.connect("data/demo.duckdb")
    con.execute(f"SET threads TO 2; SET memory_limit='512MB'; SET runtime_statistics=true;")
    con.execute(f"PRAGMA busy_timeout={TIMEOUT_MS};")
    plan = con.execute("EXPLAIN " + sql).fetchdf()
    df = con.execute(sql).fetchdf()
    con.close()
    # превью
    preview = df.head(min(len(df), 20))
    return plan.to_string(), preview
