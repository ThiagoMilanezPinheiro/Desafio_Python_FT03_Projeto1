"""Carga: Delta Lake com compressão Snappy e particionamento por data."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from src.config import is_databricks


def get_spark(app_name: str = "seismic-pipeline") -> SparkSession:
    """No Databricks reaproveita a sessão existente; localmente configura o Delta."""
    if is_databricks():
        return SparkSession.builder.getOrCreate()

    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "8")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def write_delta(df: DataFrame, path: str, partition_col: str = "event_date") -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("compression", "snappy")
        .option("overwriteSchema", "true")
        .partitionBy(partition_col)
        .save(path)
    )
