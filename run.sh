#!/usr/bin/env bash
# Executa o pipeline ponta a ponta localmente.
#   ./run.sh           -> python (delta-spark configura o Spark sozinho)
#   ./run.sh submit    -> spark-submit com o pacote Delta
# Requisitos: Python 3.10-3.11 e Java 11/17. No Windows: Git Bash + HADOOP_HOME com winutils.
set -euo pipefail
cd "$(dirname "$0")"

# Escolhe um Python compatível com PySpark 3.5 (3.10 ou 3.11)
PY="${PYTHON:-}"
if [ -z "$PY" ]; then
  for cand in python3.11 python3.10 "py -3.11" "py -3.10" python3 python; do
    if $cand -c 'import sys; assert (3,10) <= sys.version_info[:2] <= (3,11)' 2>/dev/null; then
      PY="$cand"; break
    fi
  done
fi
[ -n "$PY" ] || { echo "Python 3.10/3.11 não encontrado (defina PYTHON=...)"; exit 1; }

if [ ! -d .venv ]; then
  $PY -m venv .venv
fi
if [ -f .venv/bin/activate ]; then source .venv/bin/activate; else source .venv/Scripts/activate; fi
python -m pip install -q -r requirements.txt

[ -f .env ] || cp .env.example .env
export PYSPARK_PYTHON="$(command -v python)"

if [ "${1:-}" = "submit" ]; then
  spark-submit \
    --packages io.delta:delta-spark_2.12:3.2.0 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    src/main.py
else
  python -m src.main
fi
