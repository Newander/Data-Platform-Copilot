import math
from dataclasses import dataclass
from typing import Optional, Any, Dict, List, Tuple

import duckdb
import pandas as pd

from src.settings import DATA_DIR, DB_FILE_NAME, DQ_DEFAULT_LIMIT, DQ_MAX_LIMIT, DQ_DEFAULT_SIGMA


# ---- Fetch helpers ----
def _connect():
    con = duckdb.connect(DATA_DIR / DB_FILE_NAME)
    con.execute("SET threads TO 2; SET memory_limit='512MB';")
    return con


def fetch_table_sample(table: str, where: Optional[str] = None, limit: Optional[int] = None) -> pd.DataFrame:
    n = limit or DQ_DEFAULT_LIMIT
    n = min(max(n, 1), DQ_MAX_LIMIT)
    where_sql = f" WHERE {where} " if where and where.strip() else " "
    sql = f"SELECT * FROM {table}{where_sql}LIMIT {n}"
    con = _connect()
    try:
        df = con.execute(sql).fetchdf()
    finally:
        con.close()
    return df


# ---- Profiling ----
def _safe_len(x) -> Optional[int]:
    try:
        return len(x)
    except Exception:
        return None


def profile_df(df: pd.DataFrame, max_top: int = 5) -> Dict[str, Dict[str, Any]]:
    prof: Dict[str, Dict[str, Any]] = {}
    for col in df.columns:
        s = df[col]
        non_null = s.dropna()
        dtype = str(s.dtype)
        info: Dict[str, Any] = {
            "dtype": dtype,
            "count": int(s.shape[0]),
            "nulls": int(s.isna().sum()),
            "distinct": int(non_null.nunique()),
        }
        if pd.api.types.is_numeric_dtype(s):
            info.update({
                "min": float(non_null.min()) if not non_null.empty else None,
                "max": float(non_null.max()) if not non_null.empty else None,
                "mean": float(non_null.mean()) if not non_null.empty else None,
                "std": float(non_null.std(ddof=0)) if not non_null.empty else None,
                "p50": float(non_null.quantile(0.5)) if not non_null.empty else None,
                "p95": float(non_null.quantile(0.95)) if not non_null.empty else None,
            })
        elif pd.api.types.is_datetime64_any_dtype(s):
            info.update({
                "min_ts": non_null.min().isoformat() if not non_null.empty else None,
                "max_ts": non_null.max().isoformat() if not non_null.empty else None,
            })
        else:
            # text-like: длины
            lens = non_null.map(_safe_len).dropna()
            if not lens.empty:
                info.update({
                    "min_len": int(lens.min()),
                    "max_len": int(lens.max()),
                    "p95_len": float(pd.Series(lens).quantile(0.95)),
                })
        # top values
        vc = non_null.value_counts().head(max_top)
        info["top_values"] = [{"value": (k.isoformat() if hasattr(k, "isoformat") else k), "count": int(v)} for k, v in
                              vc.items()]
        prof[col] = info
    return prof


# ---- Simple rules engine ----
@dataclass
class RuleResult:
    rule: Dict[str, Any]
    passed: bool
    details: Dict[str, Any]


def _pct(x: int, total: int) -> float:
    return round(100.0 * x / total, 4) if total else 0.0


def check_not_null(df: pd.DataFrame, col: str) -> RuleResult:
    nulls = int(df[col].isna().sum())
    total = int(df.shape[0])
    return RuleResult({"type": "not_null", "column": col}, nulls == 0,
                      {"nulls": nulls, "total": total, "null_rate_pct": _pct(nulls, total)})


def check_unique(df: pd.DataFrame, col: str) -> RuleResult:
    s = df[col]
    total = int(s.shape[0])
    d = int(s.dropna().nunique())
    dupes = total - d
    return RuleResult({"type": "unique", "column": col}, dupes == 0,
                      {"distinct": d, "total": total, "duplicates": dupes, "dupe_rate_pct": _pct(dupes, total)})


def check_range(df: pd.DataFrame, col: str, min_val: Optional[float], max_val: Optional[float]) -> RuleResult:
    s = df[col].dropna()
    below = int((s < min_val).sum()) if min_val is not None else 0
    above = int((s > max_val).sum()) if max_val is not None else 0
    violations = below + above
    total = int(df.shape[0])
    return RuleResult({"type": "range", "column": col, "min": min_val, "max": max_val}, violations == 0, {
        "violations": violations, "below_min": below, "above_max": above, "total": total,
        "viol_rate_pct": _pct(violations, total)
    })


def check_freshness(df: pd.DataFrame, ts_col: str, max_age_hours: int) -> RuleResult:
    s = pd.to_datetime(df[ts_col], errors="coerce").dropna()
    if s.empty:
        return RuleResult({"type": "freshness", "column": ts_col, "max_age_hours": max_age_hours}, False,
                          {"error": "no timestamps"})
    latest = s.max()
    now = pd.Timestamp.utcnow()
    age_hours = float((now - latest).total_seconds() / 3600.0)
    return RuleResult({"type": "freshness", "column": ts_col, "max_age_hours": max_age_hours},
                      age_hours <= max_age_hours, {
                          "latest_iso": latest.isoformat(), "age_hours": round(age_hours, 3)
                      })


def check_anomaly_zscore(df: pd.DataFrame, col: str, sigma: float = DQ_DEFAULT_SIGMA) -> RuleResult:
    s = pd.to_numeric(df[col], errors="coerce").dropna()
    total = int(df.shape[0])
    if s.empty or s.std(ddof=0) == 0 or math.isnan(s.std(ddof=0)):
        return RuleResult({"type": "anomaly", "column": col, "method": "zscore", "sigma": sigma}, True,
                          {"skipped": "no variance or no data", "total": total})
    m = float(s.mean());
    st = float(s.std(ddof=0))
    z = (s - m).abs() / st
    outliers = int((z > sigma).sum())
    return RuleResult({"type": "anomaly", "column": col, "method": "zscore", "sigma": sigma}, outliers == 0, {
        "mean": round(m, 6), "std": round(st, 6), "sigma": sigma, "outliers": outliers, "total": total,
        "outlier_pct": _pct(outliers, total)
    })


# ---- Orchestrator ----
def run_checks(table: str, where: Optional[str], rules: List[Dict[str, Any]], sample_limit: Optional[int] = None) -> \
Tuple[Dict[str, Any], List[RuleResult], pd.DataFrame]:
    df = fetch_table_sample(table, where=where, limit=sample_limit)
    prof = profile_df(df)
    results: List[RuleResult] = []
    for r in rules:
        rtype = r.get("type")
        if rtype == "not_null":
            results.append(check_not_null(df, r["column"]))
        elif rtype == "unique":
            results.append(check_unique(df, r["column"]))
        elif rtype == "range":
            results.append(check_range(df, r["column"], r.get("min"), r.get("max")))
        elif rtype == "freshness":
            results.append(check_freshness(df, r["column"], int(r.get("max_age_hours", 24))))
        elif rtype == "anomaly":
            results.append(check_anomaly_zscore(df, r["column"], float(r.get("sigma", DQ_DEFAULT_SIGMA))))
        else:
            results.append(RuleResult({"type": rtype}, False, {"error": "unknown rule"}))
    return prof, results, df.head(min(50, len(df)))


# ---- Reporting ----
def render_markdown_report(table: str, where: Optional[str], prof: Dict[str, Any], results: List[RuleResult]) -> str:
    lines: List[str] = []
    lines.append(f"# DQ Report for `{table}`")
    if where:
        lines.append(f"_Filter:_ `{where}`")
    lines.append("")
    # Summary
    passed = sum(1 for r in results if r.passed)
    total = len(results)
    lines.append(f"**Summary:** {passed}/{total} checks passed.")
    lines.append("")
    # Results
    lines.append("## Checks")
    for r in results:
        status = "✅ PASSED" if r.passed else "❌ FAILED"
        lines.append(f"- **{status}** `{r.rule}` → `{r.details}`")
    # Schema/profile short block
    lines.append("\n## Profile (excerpt)")
    for col, info in prof.items():
        lines.append(
            f"- **{col}**: {info.get('dtype')}, nulls={info.get('nulls')}, distinct={info.get('distinct')}, top={info.get('top_values')[:3]}")
    lines.append("")
    return "\n".join(lines)
