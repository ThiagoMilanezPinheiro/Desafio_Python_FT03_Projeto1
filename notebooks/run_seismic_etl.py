# Databricks notebook source
# MAGIC %md
# MAGIC # Seismic ETL — Orquestrador
# MAGIC Executa o pipeline `src/` (Extract → Transform → Quality → Load) dentro de um Git folder do Databricks.
# MAGIC
# MAGIC Fontes: **USGS Earthquake Catalog** (sismologia) + **World Bank Open Data** (países e população).

# COMMAND ----------

# MAGIC %pip install python-dotenv requests

# COMMAND ----------

import os
import sys

# No Git folder o notebook roda em <repo>/notebooks; sobe um nível para importar src/
repo_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

# COMMAND ----------

dbutils.widgets.text("START_DATE", "2024-01-01")
dbutils.widgets.text("END_DATE", "2024-04-01")
dbutils.widgets.text("OUTPUT_PATH", "/Volumes/workspace/seismic/gold/delta_seismic_gold")
dbutils.widgets.text("DQ_REPORT_PATH", "/Volumes/workspace/seismic/gold/dq_report.json")
dbutils.widgets.dropdown("EXPORT_TO_REPO", "no", ["no", "yes"])

for key in ["START_DATE", "END_DATE", "OUTPUT_PATH", "DQ_REPORT_PATH"]:
    os.environ[key] = dbutils.widgets.get(key)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Idempotente: garante que o schema e o volume de destino existem
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.seismic;
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.seismic.gold;

# COMMAND ----------

from src.main import run

report = run(spark)
print(f"Linhas: {report['row_count']} | DQ ok: {report['passed']} "
      f"| checks: {report['checks_total']} | falhas: {report['checks_failed']}")
print("Métricas:", report["metrics"])

# COMMAND ----------

display(spark.read.format("delta").load(os.environ["OUTPUT_PATH"]).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exportar para o repositório (opcional)
# MAGIC Com `EXPORT_TO_REPO = yes`, copia a tabela Delta e o relatório do Volume para `<repo>/output/`.
# MAGIC Depois, faça **Commit & Push** pelo painel Git do Databricks para versionar o dataset no GitHub.

# COMMAND ----------

import shutil

from src.load import write_delta

if dbutils.widgets.get("EXPORT_TO_REPO") == "yes":
    dest_dir = os.path.join(repo_root, "output")
    dest_table = os.path.join(dest_dir, "delta_seismic_gold")

    # Overwrites anteriores deixam arquivos "removidos" no disco (até um VACUUM).
    # Para versionar só o estado atual, regrava a versão corrente numa área limpa.
    staging = os.environ["OUTPUT_PATH"].rstrip("/") + "_export"
    shutil.rmtree(staging, ignore_errors=True)
    write_delta(spark.read.format("delta").load(os.environ["OUTPUT_PATH"]), staging)

    shutil.rmtree(dest_table, ignore_errors=True)
    shutil.copytree(staging, dest_table)
    shutil.rmtree(staging, ignore_errors=True)
    shutil.copy2(os.environ["DQ_REPORT_PATH"], os.path.join(dest_dir, "dq_report.json"))
    for root, _, files in os.walk(dest_dir):
        for f in files:
            print(os.path.relpath(os.path.join(root, f), repo_root))
else:
    print("EXPORT_TO_REPO = no (nada copiado)")
