from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from typing import List, Dict, Any, Union
import json
import os


def spark_session() -> SparkSession:
    try:
        return SparkSession.builder.getOrCreate()
    except Exception:
        return SparkSession.builder.appName("dq_framework").getOrCreate()


def row_count(df: DataFrame) -> int:
    return df.count()


def null_percentage(df: DataFrame, col: str) -> float:
    total = df.count()
    if total == 0:
        return 0.0
    nulls = df.filter(F.col(col).isNull() | (F.col(col) == "")).count()
    return float(nulls) / float(total) * 100.0


def unique_count(df: DataFrame, cols: Union[str, List[str]]) -> int:
    if isinstance(cols, str):
        cols = [cols]
    return df.select(*cols).distinct().count()


def value_range_violations(df: DataFrame, col: str, min_val: Any = None, max_val: Any = None) -> int:
    cond = None
    if min_val is not None:
        cond = (F.col(col) < F.lit(min_val))
    if max_val is not None:
        c2 = (F.col(col) > F.lit(max_val))
        cond = c2 if cond is None else (cond | c2)
    if cond is None:
        return 0
    return df.filter(cond).count()


def regex_violations(df: DataFrame, col: str, pattern: str) -> int:
    return df.filter(~F.col(col).rlike(pattern)).count()


def run_checks_on_table(table: str, checks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    spark = spark_session()
    df = spark.table(table)
    return run_checks(df, checks, table_name=table)


def run_checks(df: DataFrame, checks: List[Dict[str, Any]], table_name: str = None) -> List[Dict[str, Any]]:
    results = []
    total_rows = row_count(df)
    for c in checks:
        kind = c.get("type")
        name = c.get("name") or kind
        column = c.get("column")
        threshold = c.get("threshold")
        details: Dict[str, Any] = {"check": name, "type": kind, "column": column}
        if kind == "row_count_gt":
            expected = c.get("value", 0)
            details.update({"expected": expected, "actual": total_rows, "passed": total_rows > expected})
        elif kind == "null_pct_lt":
            pct = null_percentage(df, column)
            details.update({"threshold_pct": threshold, "actual_pct": pct, "passed": pct < threshold})
        elif kind == "unique_keys":
            keys = c.get("columns")
            uniq = unique_count(df, keys)
            details.update({"unique_count": uniq, "passed": uniq >= c.get("expected", 1)})
        elif kind == "value_range":
            violations = value_range_violations(df, column, c.get("min"), c.get("max"))
            details.update({"violations": violations, "passed": violations == 0})
        elif kind == "regex":
            violations = regex_violations(df, column, c.get("pattern"))
            details.update({"violations": violations, "passed": violations == 0})
        else:
            details.update({"error": "unknown check type", "passed": False})
        results.append(details)

    # attach metadata
    for r in results:
        if table_name:
            r.setdefault("table", table_name)
        r.setdefault("total_rows", total_rows)

    return results


def save_report(report: List[Dict[str, Any]], out_path: str = None) -> str:
    if out_path is None:
        out_dir = os.getenv("DQ_REPORT_PATH", "./dq_reports")
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, "dq_report.json")
    else:
        os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    return out_path


def pretty_print(report: List[Dict[str, Any]]):
    print(json.dumps(report, indent=2))
