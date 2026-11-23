"""Entrypoint: Extract -> Transform -> Quality -> Load."""
from __future__ import annotations

import logging
import os
import sys

# Permite rodar como `python src/main.py` / spark-submit além de `python -m src.main`
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import load_settings  # noqa: E402
from src.extract import extract_earthquakes  # noqa: E402
from src.load import get_spark, write_delta  # noqa: E402
from src.quality import run_checks, write_report  # noqa: E402
from src.transform import to_raw_df, transform  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("seismic")


def run(spark=None) -> dict:
    settings = load_settings()
    spark = spark or get_spark()

    log.info("1/4 Extract")
    payloads = extract_earthquakes(settings)

    log.info("2/4 Transform")
    gold = transform(spark, to_raw_df(spark, payloads))
    try:
        gold = gold.cache()  # não suportado no Databricks Serverless
    except Exception:
        pass

    log.info("3/4 Data Quality")
    report = run_checks(gold)
    write_report(report, settings.dq_report_path)
    if not report["passed"]:
        raise RuntimeError(f"Data Quality falhou. Veja {settings.dq_report_path}")

    log.info("4/4 Load -> %s", settings.output_path)
    write_delta(gold, settings.output_path)
    log.info("Pipeline concluído: %s linhas", report["row_count"])
    return report


if __name__ == "__main__":
    run()
