from pathlib import Path
from typing import List, Dict

import duckdb

from src.config import settings

EVENTS_DESCR: Dict[str, str] = {
    "event_id": "Unique event identifier (surrogate PK-like)",
    "user_id": "User identifier",
    "event_type": "Categorical event kind: view/click/purchase/signup/refund",
    "amount": "Monetary amount for purchase/refund; 0 otherwise",
    "event_ts": "UTC timestamp when event happened",
    "country": "ISO-like country code",
    "device": "User device group",
    "source": "Acquisition channel",
}


def _con():
    con = duckdb.connect(settings.data.data_dir / settings.database.file_name)
    con.execute("SET threads TO 2; SET memory_limit='512MB';")
    return con


def _list_tables(con) -> List[str]:
    q = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' ORDER BY 1"
    return [r[0] for r in con.execute(q).fetchall()]


def _table_info(con, table: str):
    return con.execute(f"PRAGMA table_info('{table}')").fetchdf()


def build_markdown() -> str:
    con = _con()
    try:
        tables = _list_tables(con)
        lines: List[str] = []
        lines.append("# Data Warehouse Schema (auto-generated)\n")
        for t in tables:
            lines.append(f"## {t}\n")
            df = _table_info(con, t)
            lines.append("| column | type | pk | not_null | description |")
            lines.append("|---|---|---:|---:|---|")
            for _, row in df.iterrows():
                col = str(row["name"])
                typ = str(row["type"])
                pk = "1" if int(row["pk"]) == 1 else ""
                nn = "1" if int(row["notnull"]) == 1 else ""
                descr = ""
                if t == "events":
                    descr = EVENTS_DESCR.get(col, "")
                lines.append(f"| {col} | {typ} | {pk} | {nn} | {descr} |")
            lines.append("")
        return "\n".join(lines).strip() + "\n"
    finally:
        con.close()


def write_schema_docs(path: Path | None = None) -> str:
    md = build_markdown()
    out_path = (path or (settings.database.dir / "schema_docs.md"))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(md, encoding="utf-8")
    return str(out_path)
