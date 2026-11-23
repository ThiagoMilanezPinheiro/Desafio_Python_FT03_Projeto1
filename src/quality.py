"""Qualidade de dados: schema, null ratio, unicidade e ranges."""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

REQUIRED_COLUMNS = {
    "event_id": "string",
    "magnitude": "double",
    "latitude": "double",
    "longitude": "double",
    "depth_km": "double",
    "event_ts": "timestamp",
    "event_date": "date",
}
MAX_NULL_RATIO = 0.05
RANGES = {
    "latitude": (-90.0, 90.0),
    "longitude": (-180.0, 180.0),
    "magnitude": (-2.0, 10.0),
    "depth_km": (-10.0, 800.0),
}


def run_checks(df: DataFrame) -> dict[str, Any]:
    total = df.count()
    checks: list[dict[str, Any]] = []
    dtypes = dict(df.dtypes)

    # Schema
    for col, expected in REQUIRED_COLUMNS.items():
        actual = dtypes.get(col)
        checks.append({"check": "schema", "column": col, "expected": expected,
                       "actual": actual, "passed": actual == expected})

    # Null ratio (uma única passada pelo DataFrame)
    null_counts = df.select(
        [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in REQUIRED_COLUMNS]
    ).first().asDict()
    for col, n in null_counts.items():
        ratio = (n or 0) / total if total else 0.0
        checks.append({"check": "null_ratio", "column": col, "value": round(ratio, 4),
                       "threshold": MAX_NULL_RATIO, "passed": ratio <= MAX_NULL_RATIO})

    # Unicidade da chave
    distinct = df.select("event_id").distinct().count()
    checks.append({"check": "uniqueness", "column": "event_id", "total": total,
                   "distinct": distinct, "passed": total == distinct})

    # Ranges válidos
    for col, (lo, hi) in RANGES.items():
        bad = df.filter(F.col(col).isNotNull() & ~F.col(col).between(lo, hi)).count()
        checks.append({"check": "range", "column": col, "min": lo, "max": hi,
                       "violations": bad, "passed": bad == 0})

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "row_count": total,
        "passed": all(c["passed"] for c in checks),
        "checks": checks,
    }


def write_report(report: dict[str, Any], path: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, ensure_ascii=False)
