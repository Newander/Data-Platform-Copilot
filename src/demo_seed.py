from typing import Dict, Any

import duckdb

from src.settings import DATA_DIR, DB_FILE_NAME


def _con():
    con = duckdb.connect(DATA_DIR / DB_FILE_NAME)
    con.execute("SET threads TO 2; SET memory_limit='512MB';")
    return con


def seed_events(n_rows: int = 100_000) -> Dict[str, Any]:
    """
    Создаёт/пересобирает таблицу events с синтетическими данными за ~180 дней.
    Колонки:
      - event_id BIGINT (seq)
      - user_id BIGINT
      - event_type VARCHAR ('view','click','purchase','signup','refund')
      - amount DOUBLE (покупки/возвраты; прочее 0)
      - event_ts TIMESTAMP (равномерно в последние 180 дней)
      - country/device/source категориальные
    """
    sql = f"""
    CREATE OR REPLACE TABLE events AS
    WITH base AS (
      SELECT
        i::BIGINT AS event_id,
        CAST(1 + floor(random()*1000000) AS BIGINT) AS user_id,
        random() AS r1,
        random() AS r2,
        random() AS r3,
        random() AS r4,
        random() AS r5
      FROM range({n_rows}) t(i)
    )
    SELECT
      event_id,
      user_id,
      CASE
        WHEN r1 < 0.50 THEN 'view'
        WHEN r1 < 0.80 THEN 'click'
        WHEN r1 < 0.95 THEN 'purchase'
        WHEN r1 < 0.98 THEN 'signup'
        ELSE 'refund'
      END AS event_type,
      CASE
        WHEN r1 >= 0.80 AND r1 < 0.95 THEN round(r5 * 200, 2)        -- purchase
        WHEN r1 >= 0.98 THEN round(r5 * 100, 2)                       -- refund
        ELSE 0
      END AS amount,
      dateadd('second', -CAST(floor(random()*86400) AS INTEGER),
        dateadd('day', -CAST(floor(random()*180) AS INTEGER), now())
      )::TIMESTAMP AS event_ts,
      CASE
        WHEN r2 < 0.25 THEN 'PL'
        WHEN r2 < 0.45 THEN 'DE'
        WHEN r2 < 0.60 THEN 'FR'
        WHEN r2 < 0.75 THEN 'US'
        WHEN r2 < 0.90 THEN 'GB'
        ELSE 'ES'
      END AS country,
      CASE
        WHEN r3 < 0.70 THEN 'mobile'
        WHEN r3 < 0.90 THEN 'desktop'
        ELSE 'tablet'
      END AS device,
      CASE
        WHEN r4 < 0.30 THEN 'search'
        WHEN r4 < 0.55 THEN 'ads'
        WHEN r4 < 0.75 THEN 'direct'
        WHEN r4 < 0.90 THEN 'social'
        ELSE 'email'
      END AS source
    FROM base;
    """
    con = _con()
    try:
        con.execute(sql)
        cnt = con.execute("SELECT COUNT(*) AS c FROM events").fetchone()[0]
        min_ts, max_ts = con.execute("SELECT min(event_ts), max(event_ts) FROM events").fetchone()
    finally:
        con.close()
    return {"table": "events", "rows": int(cnt), "min_ts": str(min_ts), "max_ts": str(max_ts)}
