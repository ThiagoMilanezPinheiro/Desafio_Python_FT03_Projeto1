"""Transformação: PySpark + Spark SQL.

Camadas:
  bronze  -> JSON bruto das APIs, parseado com schema explícito (from_json)
  silver  -> desaninhado (EXPLODE em SQL), tipado, sem nulos críticos e deduplicado
  gold    -> terremotos cruzados (JOIN) com países/população do World Bank + métricas

Transformações aplicadas (requisito: no mínimo 5):
  1. Desaninhamento de arrays/structs via SQL (LATERAL VIEW EXPLODE)
  2. Padronização de tipos e datas (epoch ms -> timestamp, strings -> double/int)
  3. Tratamento de nulos (descarte de registros sem chave/data/magnitude + COALESCE)
  4. Deduplicação (ROW_NUMBER pela versão mais recente de cada evento)
  5. Cruzamento de fontes (JOIN USGS x World Bank países x World Bank população)
  6. Enriquecimento de métricas (energia liberada, classes, eventos por milhão de hab.)
"""
from __future__ import annotations

import json
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# ---------------------------------------------------------------------------
# Schemas explícitos (governança: nada de inferência de schema em produção)
# ---------------------------------------------------------------------------
USGS_SCHEMA = StructType([
    StructField("features", ArrayType(StructType([
        StructField("id", StringType()),
        StructField("properties", StructType([
            StructField("mag", DoubleType()),
            StructField("magType", StringType()),
            StructField("place", StringType()),
            StructField("time", LongType()),
            StructField("updated", LongType()),
            StructField("tsunami", IntegerType()),
            StructField("type", StringType()),
        ])),
        StructField("geometry", StructType([
            StructField("coordinates", ArrayType(DoubleType())),
        ])),
    ]))),
])

_ID_VALUE = StructType([StructField("id", StringType()), StructField("value", StringType())])

WB_COUNTRY_SCHEMA = StructType([
    StructField("id", StringType()),
    StructField("iso2Code", StringType()),
    StructField("name", StringType()),
    StructField("region", _ID_VALUE),
    StructField("incomeLevel", _ID_VALUE),
    StructField("capitalCity", StringType()),
    StructField("longitude", StringType()),
    StructField("latitude", StringType()),
])

WB_POPULATION_SCHEMA = StructType([
    StructField("countryiso3code", StringType()),
    StructField("country", _ID_VALUE),
    StructField("date", StringType()),
    StructField("value", LongType()),
])

# Nomes usados no campo `place` da USGS que diferem do cadastro do World Bank
_US_STATES = [
    "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut",
    "Delaware", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa",
    "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan",
    "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire",
    "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio",
    "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota",
    "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington", "West Virginia",
    "Wisconsin", "Wyoming",
    "AK", "AZ", "CA", "CO", "HI", "ID", "MT", "NM", "NV", "OK", "OR", "TX", "UT", "WA", "WY",
]
COUNTRY_ALIASES: dict[str, str] = {
    **{state: "USA" for state in _US_STATES},
    "MX": "MEX", "Russia": "RUS", "Kuril Islands": "RUS", "Iran": "IRN", "Turkey": "TUR",
    "Türkiye": "TUR", "Venezuela": "VEN", "South Korea": "KOR", "North Korea": "PRK",
    "Egypt": "EGY", "Syria": "SYR", "Laos": "LAO", "Vietnam": "VNM", "Kyrgyzstan": "KGZ",
    "Slovakia": "SVK", "Yemen": "YEM", "Micronesia": "FSM",
    "Federated States of Micronesia": "FSM", "Democratic Republic of the Congo": "COD",
    "Congo": "COG", "Burma": "MMR", "U.S. Virgin Islands": "VIR", "Bahamas": "BHS",
    "Gambia": "GMB", "Brunei": "BRN", "Hong Kong": "HKG", "Saint Lucia": "LCA",
    "Saint Kitts and Nevis": "KNA", "Saint Vincent and the Grenadines": "VCT",
    "Cape Verde": "CPV", "Ivory Coast": "CIV", "Czech Republic": "CZE", "East Timor": "TLS",
    "Timor Leste": "TLS", "Fiji Islands": "FJI", "Kermadec Islands": "NZL",
    "Puerto Rico": "PRI", "Mariana Islands": "MNP", "Northern Mariana Islands": "MNP",
    "Loyalty Islands": "NCL",
}

MAGNITUDE_CLASSES: list[tuple[str, float, float]] = [
    ("micro", -10.0, 3.0), ("minor", 3.0, 4.0), ("light", 4.0, 5.0),
    ("moderate", 5.0, 6.0), ("strong", 6.0, 7.0), ("major", 7.0, 8.0), ("great", 8.0, 99.0),
]


# ---------------------------------------------------------------------------
# Bronze
# ---------------------------------------------------------------------------
def _json_rows_to_df(spark: SparkSession, records: list[Any], schema: StructType) -> DataFrame:
    """Converte registros JSON em DataFrame via from_json (sem sparkContext).

    Funciona no Spark local e no Databricks Serverless / Spark Connect.
    """
    raw = spark.createDataFrame([(json.dumps(r),) for r in records], "raw STRING")
    return raw.select(F.from_json("raw", schema).alias("j")).select("j.*")


def register_bronze(
    spark: SparkSession,
    usgs_payloads: list[dict[str, Any]],
    worldbank: dict[str, list[dict[str, Any]]],
) -> None:
    _json_rows_to_df(spark, usgs_payloads, USGS_SCHEMA).createOrReplaceTempView("bronze_usgs")
    _json_rows_to_df(spark, worldbank["countries"], WB_COUNTRY_SCHEMA) \
        .createOrReplaceTempView("bronze_wb_countries")
    _json_rows_to_df(spark, worldbank["population"], WB_POPULATION_SCHEMA) \
        .createOrReplaceTempView("bronze_wb_population")

    spark.createDataFrame(list(COUNTRY_ALIASES.items()), "alias STRING, iso3 STRING") \
        .createOrReplaceTempView("dim_country_alias")
    spark.createDataFrame(MAGNITUDE_CLASSES, "mag_class STRING, mag_min DOUBLE, mag_max DOUBLE") \
        .createOrReplaceTempView("dim_magnitude")


# ---------------------------------------------------------------------------
# Silver
# ---------------------------------------------------------------------------
SQL_SILVER_EARTHQUAKES = """
WITH exploded AS (                      -- (1) desaninhamento: 1 linha por evento
    SELECT f.id                       AS event_id,
           f.properties.mag           AS mag,
           f.properties.magType       AS mag_type,
           f.properties.place         AS place,
           f.properties.time          AS time_ms,
           f.properties.updated       AS updated_ms,
           f.properties.tsunami       AS tsunami,
           f.properties.type          AS event_type,
           f.geometry.coordinates[0]  AS longitude,
           f.geometry.coordinates[1]  AS latitude,
           f.geometry.coordinates[2]  AS depth_km
    FROM bronze_usgs
    LATERAL VIEW EXPLODE(features) t AS f
),
typed AS (                              -- (2) tipos/datas  (3) nulos
    SELECT TRIM(event_id)                                          AS event_id,
           CAST(mag AS DOUBLE)                                     AS magnitude,
           LOWER(COALESCE(NULLIF(TRIM(mag_type), ''), 'unknown'))  AS mag_type,
           COALESCE(NULLIF(TRIM(place), ''), 'Unknown')            AS place,
           timestamp_millis(time_ms)                               AS event_ts,
           timestamp_millis(updated_ms)                            AS updated_ts,
           CAST(COALESCE(tsunami, 0) AS INT)                       AS tsunami,
           LOWER(event_type)                                       AS event_type,
           CAST(longitude AS DOUBLE)                               AS longitude,
           CAST(latitude  AS DOUBLE)                               AS latitude,
           CAST(depth_km  AS DOUBLE)                               AS depth_km
    FROM exploded
    WHERE event_id IS NOT NULL AND time_ms IS NOT NULL AND mag IS NOT NULL
      AND longitude IS NOT NULL AND latitude IS NOT NULL
),
ranked AS (                             -- (4) deduplicação: versão mais recente
    SELECT *, ROW_NUMBER() OVER (
               PARTITION BY event_id ORDER BY updated_ts DESC NULLS LAST) AS rn
    FROM typed
)
SELECT event_id, magnitude, mag_type, place, event_ts, updated_ts, tsunami,
       longitude, latitude, depth_km,
       -- região textual: último trecho de "12 km NE of Cidade, País"
       regexp_replace(TRIM(element_at(split(place, ', '), -1)), '(?i) region$', '')
           AS place_region
FROM ranked
WHERE rn = 1 AND event_type = 'earthquake'
"""

SQL_SILVER_COUNTRIES = """
SELECT id                                         AS iso3,
       iso2Code                                   AS iso2,
       TRIM(name)                                 AS country_name,
       TRIM(region.value)                         AS wb_region,      -- struct aninhado
       TRIM(incomeLevel.value)                    AS income_level,   -- struct aninhado
       NULLIF(TRIM(capitalCity), '')              AS capital_city,
       CAST(NULLIF(latitude, '')  AS DOUBLE)      AS capital_lat,
       CAST(NULLIF(longitude, '') AS DOUBLE)      AS capital_lon
FROM bronze_wb_countries
WHERE TRIM(region.value) <> 'Aggregates'        -- remove agregados (ex.: "World")
"""

SQL_SILVER_POPULATION = """
SELECT iso3, population, population_year
FROM (
    SELECT countryiso3code         AS iso3,
           value                   AS population,
           CAST(date AS INT)       AS population_year,
           ROW_NUMBER() OVER (PARTITION BY countryiso3code ORDER BY CAST(date AS INT) DESC) AS rn
    FROM bronze_wb_population
    WHERE value IS NOT NULL AND NULLIF(countryiso3code, '') IS NOT NULL
)
WHERE rn = 1                            -- ano mais recente com valor disponível
"""

# ---------------------------------------------------------------------------
# Gold
# ---------------------------------------------------------------------------
SQL_GOLD = """
WITH matched AS (                       -- (5) cruzamento de fontes
    SELECT q.*,
           COALESCE(a.iso3, cn.iso3) AS country_iso3
    FROM silver_earthquakes q
    LEFT JOIN dim_country_alias a  ON LOWER(q.place_region) = LOWER(a.alias)
    LEFT JOIN silver_countries  cn ON LOWER(q.place_region) = LOWER(cn.country_name)
),
enriched AS (
    SELECT m.*,
           c.country_name, c.wb_region, c.income_level,
           p.population AS country_population, p.population_year,
           d.mag_class,
           YEAR(m.event_ts)  AS event_year,
           MONTH(m.event_ts) AS event_month,
           TO_DATE(m.event_ts) AS event_date
    FROM matched m
    LEFT JOIN silver_countries  c ON m.country_iso3 = c.iso3
    LEFT JOIN silver_population p ON m.country_iso3 = p.iso3
    LEFT JOIN dim_magnitude     d ON m.magnitude >= d.mag_min AND m.magnitude < d.mag_max
)
SELECT                                  -- (6) enriquecimento de métricas
    event_id, event_ts, event_date, event_year, event_month, updated_ts,
    magnitude, mag_type, mag_class,
    depth_km,
    CASE WHEN depth_km < 70  THEN 'shallow'
         WHEN depth_km < 300 THEN 'intermediate'
         WHEN depth_km IS NULL THEN 'unknown'
         ELSE 'deep' END                                        AS depth_class,
    latitude, longitude, place, place_region, tsunami,
    country_iso3,
    COALESCE(country_name, 'Unmatched / Oceanic')               AS country_name,
    COALESCE(wb_region, 'Unknown')                              AS wb_region,
    COALESCE(income_level, 'Unknown')                           AS income_level,
    country_population, population_year,
    -- energia sísmica (Gutenberg-Richter): log10(E) = 1.5*M + 4.8  [joules]
    POWER(10, 1.5 * magnitude + 4.8)                            AS energy_joules,
    (magnitude >= 5.0 OR tsunami = 1)                           AS is_significant,
    CASE WHEN country_iso3 IS NOT NULL THEN
        COUNT(*) OVER (PARTITION BY country_iso3, event_year, event_month) END
                                                                AS country_month_events,
    CASE WHEN country_iso3 IS NOT NULL AND country_population > 0 THEN
        ROUND(COUNT(*) OVER (PARTITION BY country_iso3, event_year, event_month)
              / (country_population / 1e6), 4) END              AS events_per_million_pop
FROM enriched
"""


def transform(
    spark: SparkSession,
    usgs_payloads: list[dict[str, Any]],
    worldbank: dict[str, list[dict[str, Any]]],
) -> DataFrame:
    register_bronze(spark, usgs_payloads, worldbank)
    spark.sql(SQL_SILVER_EARTHQUAKES).createOrReplaceTempView("silver_earthquakes")
    spark.sql(SQL_SILVER_COUNTRIES).createOrReplaceTempView("silver_countries")
    spark.sql(SQL_SILVER_POPULATION).createOrReplaceTempView("silver_population")
    return spark.sql(SQL_GOLD)


def layer_counts(spark: SparkSession) -> dict[str, int]:
    """Contagens por camada para o relatório de auditoria."""
    return {
        "bronze_usgs_events": spark.sql(
            "SELECT COALESCE(SUM(SIZE(features)), 0) AS n FROM bronze_usgs").first()["n"],
        "silver_earthquakes": spark.table("silver_earthquakes").count(),
        "silver_countries": spark.table("silver_countries").count(),
        "silver_population": spark.table("silver_population").count(),
    }
