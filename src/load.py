"""Carga: Delta Lake com compressão Snappy e particionamento por ano/mês."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from src.config import is_databricks

# Datas/partições sempre em UTC: o resultado não pode depender do fuso da máquina
SESSION_TIMEZONE = "UTC"
PARTITION_COLS: list[str] = ["event_year", "event_month"]


def get_spark(app_name: str = "seismic-pipeline") -> SparkSession:
    """No Databricks reaproveita a sessão existente; localmente configura o Delta."""
    if is_databricks():
        spark = SparkSession.builder.getOrCreate()
    else:
        from delta import configure_spark_with_delta_pip

        builder = (
            SparkSession.builder.appName(app_name)
            .master("local[*]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.sql.session.timeZone", SESSION_TIMEZONE)
        )
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    configure_session(spark)
    return spark


def configure_session(spark: SparkSession) -> None:
    """Fixa o fuso da sessão (também aplicado à sessão recebida do notebook)."""
    spark.conf.set("spark.sql.session.timeZone", SESSION_TIMEZONE)


def write_delta(df: DataFrame, path: str, partition_cols: list[str] | None = None) -> None:
    """Grava a camada Gold em Delta Lake (Parquet + Snappy), particionada por ano/mês.

    O repartition pelas colunas de partição gera 1 arquivo por partição,
    evitando o problema de "small files".
    """
    cols = partition_cols or PARTITION_COLS
    (
        df.repartition(*cols)
        .write.format("delta")
        .mode("overwrite")
        .option("compression", "snappy")
        .option("overwriteSchema", "true")
        .partitionBy(*cols)
        .save(path)
    )
