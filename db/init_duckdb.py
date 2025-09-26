import logging

import duckdb
import pandas as pd

from src.constants import DATA_DIR

if __name__ == '__main__':
    db_path = DATA_DIR / "demo.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute("INSTALL httpfs; LOAD httpfs;")

    for name in ("customers", "orders", "items"):
        df = pd.read_csv(DATA_DIR / f"{name}.csv")
        con.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM df")

    con.execute("""
                CREATE
                OR REPLACE VIEW v_revenue_by_country AS
                SELECT c.country, SUM(o.total_amount) AS revenue
                FROM orders o
                         JOIN customers c USING (customer_id)
                GROUP BY 1
                """)
    con.close()
    logging.info("DuckDB initialized:", db_path)
