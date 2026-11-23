#!/usr/bin/env bash
# Executa o pipeline ponta a ponta localmente.
# Uso: ./run.sh            -> usa python (delta-spark configura o Spark sozinho)
#      ./run.sh submit     -> usa spark-submit com o pacote Delta
set -euo pipefail

cd "$(dirname "$0")"

if [ ! -d ".venv" ]; then
  python -m venv .venv
fi
# shellcheck disable=SC1091
source .venv/bin/activate 2>/dev/null || source .venv/Scripts/activate
pip install -q -r requirements.txt

[ -f .env ] || cp .env.example .env

if [ "${1:-}" = "submit" ]; then
  spark-submit \
    --packages io.delta:delta-spark_2.12:3.2.0 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    src/main.py
else
  python -m src.main
fi
