# Databricks notebook source
# MAGIC %md
# MAGIC # Seismic ETL — Orquestrador
# MAGIC Executa o pipeline `src/` (Extract → Transform → Quality → Load) dentro do Databricks Repos.

# COMMAND ----------

# MAGIC %pip install python-dotenv requests

# COMMAND ----------

import os
import sys

# No Databricks Repos o notebook roda em <repo>/notebooks; sobe um nível para importar src/
repo_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

# COMMAND ----------

dbutils.widgets.text("START_DATE", "2024-01-01")
dbutils.widgets.text("END_DATE", "2024-01-07")
dbutils.widgets.text("OUTPUT_PATH", "/Volumes/workspace/seismic/gold/delta_seismic_gold")
dbutils.widgets.text("DQ_REPORT_PATH", "/Volumes/workspace/seismic/gold/dq_report.json")

for key in ["START_DATE", "END_DATE", "OUTPUT_PATH", "DQ_REPORT_PATH"]:
    os.environ[key] = dbutils.widgets.get(key)

# COMMAND ----------

from src.main import run

report = run(spark)
print(f"Linhas: {report['row_count']} | DQ ok: {report['passed']}")

# COMMAND ----------

display(spark.read.format("delta").load(os.environ["OUTPUT_PATH"]).limit(20))
