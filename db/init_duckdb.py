import os
from pathlib import Path

import duckdb
import pandas as pd

if __name__ == '__main__':
    data_path = Path(os.getenv("DATA_DIR"))
    db_path = data_path / "demo.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute("INSTALL httpfs; LOAD httpfs;")

    for name in ("customers", "orders", "items"):
        df = pd.read_csv(data_path / f"{name}.csv")
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
    print("DuckDB initialized:", db_path)
