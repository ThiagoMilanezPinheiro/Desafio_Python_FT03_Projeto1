"""Configuração centralizada.

Ordem de resolução de cada parâmetro:
  1. dbutils.secrets (quando rodando no Databricks e a chave existir no scope)
  2. variável de ambiente (os.getenv / arquivo .env carregado via python-dotenv)
  3. valor padrão seguro definido aqui
"""
from __future__ import annotations

import os
from dataclasses import dataclass

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:  # no Databricks o python-dotenv pode não estar instalado
    pass


def _get_dbutils():
    """Retorna o dbutils se estiver no Databricks, senão None."""
    try:
        from pyspark.dbutils import DBUtils  # type: ignore
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        return DBUtils(spark) if spark else None
    except Exception:
        return None


def get_setting(key: str, default: str | None = None) -> str | None:
    """Busca primeiro no secret scope do Databricks e cai para o os.getenv."""
    dbutils = _get_dbutils()
    if dbutils is not None:
        scope = os.getenv("DATABRICKS_SECRET_SCOPE", "seismic")
        try:
            return dbutils.secrets.get(scope=scope, key=key)
        except Exception:
            pass  # chave não existe no scope -> fallback
    return os.getenv(key, default)


def is_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


@dataclass(frozen=True)
class Settings:
    api_base_url: str
    api_timeout: int
    api_max_retries: int
    api_backoff_factor: float
    api_rate_limit_per_sec: float
    start_date: str
    end_date: str
    min_magnitude: float
    output_path: str
    dq_report_path: str


def load_settings() -> Settings:
    return Settings(
        api_base_url=get_setting(
            "API_BASE_URL", "https://earthquake.usgs.gov/fdsnws/event/1/query"
        ),
        api_timeout=int(get_setting("API_TIMEOUT", "30")),
        api_max_retries=int(get_setting("API_MAX_RETRIES", "5")),
        api_backoff_factor=float(get_setting("API_BACKOFF_FACTOR", "2")),
        api_rate_limit_per_sec=float(get_setting("API_RATE_LIMIT_PER_SEC", "2")),
        start_date=get_setting("START_DATE", "2024-01-01"),
        end_date=get_setting("END_DATE", "2024-01-07"),
        min_magnitude=float(get_setting("MIN_MAGNITUDE", "2.5")),
        output_path=get_setting("OUTPUT_PATH", "output/delta_seismic_gold"),
        dq_report_path=get_setting("DQ_REPORT_PATH", "output/dq_report.json"),
    )
