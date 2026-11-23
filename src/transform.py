"""Transformação: PySpark + Spark SQL (EXPLODE, JOIN e 5+ transformações)."""
from __future__ import annotations

import json
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def to_raw_df(spark: SparkSession, payloads: list[dict[str, Any]]) -> DataFrame:
    """Bronze: um registro por resposta da API, com o array `features` aninhado."""
    rdd = spark.sparkContext.parallelize([json.dumps(p) for p in payloads])
    return spark.read.json(rdd)


def build_magnitude_dim(spark: SparkSession) -> DataFrame:
    """Dimensão de classes de magnitude (escala de Richter) usada no JOIN."""
    rows = [
        ("micro", 0.0, 3.0),
        ("minor", 3.0, 4.0),
        ("light", 4.0, 5.0),
        ("moderate", 5.0, 6.0),
        ("strong", 6.0, 7.0),
        ("major", 7.0, 8.0),
        ("great", 8.0, 99.0),
    ]
    return spark.createDataFrame(rows, "mag_class STRING, mag_min DOUBLE, mag_max DOUBLE")


def transform(spark: SparkSession, raw: DataFrame) -> DataFrame:
    # 1) EXPLODE: um evento por linha
    events = raw.select(F.explode("features").alias("f"))

    # 2) Flatten / seleção de colunas aninhadas + coordenadas do array geometry
    flat = events.select(
        F.col("f.id").alias("event_id"),
        F.col("f.properties.mag").cast("double").alias("magnitude"),
        F.col("f.properties.magType").alias("mag_type"),
        F.col("f.properties.place").alias("place"),
        F.col("f.properties.time").alias("time_ms"),
        F.col("f.properties.tsunami").cast("int").alias("tsunami"),
        F.col("f.properties.type").alias("event_type"),
        F.col("f.geometry.coordinates")[0].cast("double").alias("longitude"),
        F.col("f.geometry.coordinates")[1].cast("double").alias("latitude"),
        F.col("f.geometry.coordinates")[2].cast("double").alias("depth_km"),
    )

    # 3) Conversão de tipos: epoch ms -> timestamp + coluna de partição
    typed = flat.withColumn(
        "event_ts", (F.col("time_ms") / 1000).cast("timestamp")
    ).withColumn("event_date", F.to_date("event_ts")).drop("time_ms")

    # 4) Deduplicação (janelas podem se sobrepor nas bordas)
    dedup = typed.dropDuplicates(["event_id"])

    # 5) Enriquecimento via Spark SQL: região extraída do texto + faixa de profundidade
    dedup.createOrReplaceTempView("events")
    build_magnitude_dim(spark).createOrReplaceTempView("dim_magnitude")

    # 6) JOIN com a dimensão de magnitude (range join)
    return spark.sql(
        """
        SELECT
            e.*,
            TRIM(COALESCE(element_at(split(e.place, ', '), -1), 'Unknown')) AS region,
            CASE
                WHEN e.depth_km < 70  THEN 'shallow'
                WHEN e.depth_km < 300 THEN 'intermediate'
                ELSE 'deep'
            END AS depth_class,
            d.mag_class
        FROM events e
        LEFT JOIN dim_magnitude d
          ON e.magnitude >= d.mag_min AND e.magnitude < d.mag_max
        WHERE e.event_type = 'earthquake'
        """
    )
