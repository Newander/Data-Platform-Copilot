import logging

import duckdb
import pandas as pd

from src.config import settings

if __name__ == '__main__':
    db_path = settings.data.data_dir / settings.database.file_name
    con = duckdb.connect(str(db_path))
    con.execute("INSTALL httpfs; LOAD httpfs;")

    for name in ("customers", "orders", "items"):
        df = pd.read_csv(settings.data.data_dir / f"{name}.csv")
        con.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM df")

    con.execute("""
                CREATE OR REPLACE VIEW v_revenue_by_country AS
                SELECT c.country, SUM(o.total_amount) AS revenue
                FROM orders o
                         JOIN customers c USING (customer_id)
                GROUP BY 1
                """)
    con.close()
    logging.info("DuckDB initialized:", db_path)
