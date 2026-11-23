"""Data Quality: schema, null ratio, unicidade, intervalos e métricas de consistência."""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Contrato da camada Gold: coluna -> tipo Spark esperado
EXPECTED_SCHEMA: dict[str, str] = {
    "event_id": "string",
    "event_ts": "timestamp",
    "event_date": "date",
    "event_year": "int",
    "event_month": "int",
    "magnitude": "double",
    "mag_class": "string",
    "depth_km": "double",
    "latitude": "double",
    "longitude": "double",
    "tsunami": "int",
    "country_name": "string",
    "energy_joules": "double",
}
# Null ratio máximo tolerado por coluna (0.0 = obrigatória)
NULL_THRESHOLDS: dict[str, float] = {
    "event_id": 0.0,
    "event_ts": 0.0,
    "magnitude": 0.0,
    "latitude": 0.0,
    "longitude": 0.0,
    "mag_class": 0.0,
    "depth_km": 0.05,
    "country_iso3": 0.80,  # eventos oceânicos não têm país: apenas monitoramos
}
RANGES: dict[str, tuple[float, float]] = {
    "latitude": (-90.0, 90.0),
    "longitude": (-180.0, 180.0),
    "magnitude": (-2.0, 10.0),
    "depth_km": (-10.0, 800.0),
    "event_month": (1, 12),
    "tsunami": (0, 1),
}


def _check(name: str, column: str | None, passed: bool, **details: Any) -> dict[str, Any]:
    return {"check": name, "column": column, "passed": bool(passed), **details}


def check_schema(df: DataFrame) -> list[dict[str, Any]]:
    actual = dict(df.dtypes)
    return [
        _check("schema", col, actual.get(col) == expected, expected=expected,
               actual=actual.get(col))
        for col, expected in EXPECTED_SCHEMA.items()
    ]


def check_null_ratio(df: DataFrame, total: int) -> list[dict[str, Any]]:
    cols = [c for c in NULL_THRESHOLDS if c in df.columns]
    nulls = df.select(
        [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in cols]
    ).first().asDict()
    out = []
    for col in cols:
        ratio = (nulls[col] or 0) / total if total else 0.0
        out.append(_check("null_ratio", col, ratio <= NULL_THRESHOLDS[col],
                          value=round(ratio, 4), threshold=NULL_THRESHOLDS[col]))
    return out


def check_uniqueness(df: DataFrame, total: int, key: str = "event_id") -> list[dict[str, Any]]:
    distinct = df.select(key).distinct().count()
    return [_check("uniqueness", key, total == distinct, total=total, distinct=distinct,
                   duplicates=total - distinct)]


def check_ranges(df: DataFrame) -> list[dict[str, Any]]:
    out = []
    for col, (lo, hi) in RANGES.items():
        bad = df.filter(F.col(col).isNotNull() & ~F.col(col).between(lo, hi)).count()
        out.append(_check("range", col, bad == 0, min=lo, max=hi, violations=bad))
    return out


def consistency_metrics(df: DataFrame, total: int) -> dict[str, Any]:
    """Métricas descritivas para auditoria (não bloqueiam o pipeline)."""
    agg = df.agg(
        F.min("event_ts").alias("min_event_ts"),
        F.max("event_ts").alias("max_event_ts"),
        F.round(F.avg("magnitude"), 3).alias("avg_magnitude"),
        F.max("magnitude").alias("max_magnitude"),
        F.sum(F.col("country_iso3").isNotNull().cast("int")).alias("matched"),
        F.countDistinct("country_iso3").alias("distinct_countries"),
        F.sum(F.col("is_significant").cast("int")).alias("significant_events"),
    ).first().asDict()
    by_month = {
        f"{r['event_year']}-{r['event_month']:02d}": r["n"]
        for r in df.groupBy("event_year", "event_month").agg(F.count("*").alias("n"))
                   .orderBy("event_year", "event_month").collect()
    }
    top_countries = {
        r["country_name"]: r["n"]
        for r in df.filter(F.col("country_iso3").isNotNull())
                   .groupBy("country_name").agg(F.count("*").alias("n"))
                   .orderBy(F.desc("n")).limit(10).collect()
    }
    return {
        "event_ts_range": [str(agg["min_event_ts"]), str(agg["max_event_ts"])],
        "avg_magnitude": agg["avg_magnitude"],
        "max_magnitude": agg["max_magnitude"],
        "significant_events": agg["significant_events"],
        "country_match_rate": round((agg["matched"] or 0) / total, 4) if total else 0.0,
        "distinct_countries": agg["distinct_countries"],
        "rows_per_partition": by_month,
        "top_countries": top_countries,
    }


def run_checks(df: DataFrame) -> dict[str, Any]:
    total = df.count()
    checks = (
        [_check("row_count", None, total > 0, value=total)]
        + check_schema(df)
        + check_null_ratio(df, total)
        + check_uniqueness(df, total)
        + check_ranges(df)
    )
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "row_count": total,
        "passed": all(c["passed"] for c in checks),
        "checks_total": len(checks),
        "checks_failed": sum(not c["passed"] for c in checks),
        "checks": checks,
        "metrics": consistency_metrics(df, total) if total else {},
    }


def write_report(report: dict[str, Any], path: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, ensure_ascii=False, default=str)
