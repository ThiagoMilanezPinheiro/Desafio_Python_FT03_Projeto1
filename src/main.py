"""Entrypoint: Extract -> Transform -> Data Quality -> Load."""
from __future__ import annotations

import logging
import os
import sys
import time
from typing import Any

# Permite rodar como `python src/main.py` / spark-submit além de `python -m src.main`
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession  # noqa: E402

from src.config import load_settings  # noqa: E402
from src.extract import RateLimitedClient, extract_earthquakes, extract_worldbank  # noqa: E402
from src.load import configure_session, get_spark, write_delta  # noqa: E402
from src.quality import run_checks, write_report  # noqa: E402
from src.transform import layer_counts, transform  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("seismic")


def run(spark: SparkSession | None = None) -> dict[str, Any]:
    started = time.time()
    settings = load_settings()
    spark = spark or get_spark()
    configure_session(spark)  # garante UTC também na sessão vinda do notebook
    client = RateLimitedClient(settings)

    log.info("1/4 Extract (USGS + World Bank)")
    usgs = extract_earthquakes(client, settings)
    worldbank = extract_worldbank(client, settings)

    log.info("2/4 Transform (Spark SQL)")
    gold = transform(spark, usgs, worldbank)
    try:
        gold = gold.cache()  # não suportado no Databricks Serverless
    except Exception:
        pass

    log.info("3/4 Data Quality")
    report = run_checks(gold)
    report["execution"] = {
        "parameters": {
            "start_date": settings.start_date,
            "end_date": settings.end_date,
            "min_magnitude": settings.min_magnitude,
            "population_years": settings.population_years,
        },
        "sources": {
            "usgs": settings.usgs_api_url,
            "worldbank": settings.worldbank_api_url,
        },
        "http": {
            "requests": client.stats.requests,
            "retries": client.stats.retries,
            "failures": client.stats.failures,
        },
        "layer_counts": layer_counts(spark),
        "output_path": settings.output_path,
    }
    if not report["passed"]:
        report["execution"]["duration_s"] = round(time.time() - started, 1)
        write_report(report, settings.dq_report_path)
        raise RuntimeError(f"Data Quality falhou. Veja {settings.dq_report_path}")

    log.info("4/4 Load -> %s", settings.output_path)
    write_delta(gold, settings.output_path)
    report["execution"]["duration_s"] = round(time.time() - started, 1)
    write_report(report, settings.dq_report_path)
    log.info("Pipeline concluído: %s linhas | DQ ok", report["row_count"])
    return report


if __name__ == "__main__":
    run()
