# Seismic Pipeline — Databricks & Local

Pipeline ETL que extrai eventos sísmicos da API pública do **USGS Earthquake Catalog**, transforma com **PySpark + Spark SQL**, valida a qualidade dos dados e grava a camada **Gold** em **Delta Lake** (Snappy, particionada por `event_date`).

## Arquitetura

```
USGS API ──► extract.py ──► transform.py ──► quality.py ──► load.py ──► output/delta_seismic_gold
 (GeoJSON)   rate limit +    EXPLODE, JOIN,    schema, nulos,  Delta +       + output/dq_report.json
             backoff         Spark SQL         unicidade,      Snappy +
                                               ranges          partição
```

## Estrutura

| Caminho | Responsabilidade |
|---|---|
| `src/config.py` | Lê configurações de `dbutils.secrets` → `os.getenv`/`.env` → padrão |
| `src/extract.py` | Cliente HTTP com rate limiting e exponential backoff com jitter |
| `src/transform.py` | EXPLODE de `features`, flatten, tipagem, dedup, Spark SQL e JOIN com dimensão de magnitude |
| `src/quality.py` | Checagens de schema, proporção de nulos, unicidade e faixas válidas; gera `dq_report.json` |
| `src/load.py` | Cria a SparkSession (local ou Databricks) e escreve em Delta |
| `src/main.py` | Entrypoint que orquestra as etapas |
| `notebooks/run_seismic_etl.py` | Notebook (formato fonte Databricks) que chama `src.main.run(spark)` |

## API de origem

- Endpoint: `https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson`
- Parâmetros usados: `starttime`, `endtime`, `minmagnitude`
- Limite: 20.000 eventos por chamada → a extração é dividida em janelas diárias.

## Como rodar localmente

Pré-requisitos: Python 3.10+ e Java 8/11/17 (necessário para o Spark).

```bash
cp .env.example .env     # ajuste as datas, se quiser
./run.sh                 # cria a venv, instala as dependências e executa
./run.sh submit          # alternativa via spark-submit
```

No Windows, rode o `run.sh` pelo Git Bash ou WSL.

## Como rodar no Databricks

1. **Workspace → Repos → Add Repo** e cole a URL deste GitHub.
2. Abra `notebooks/run_seismic_etl.py` (o Databricks o reconhece como notebook).
3. Ajuste os widgets (`START_DATE`, `END_DATE`, `OUTPUT_PATH`) e rode **Run all**.
4. (Opcional) Guarde valores sensíveis num secret scope:
   `databricks secrets create-scope seismic` — o `config.py` os lê automaticamente.
5. (Opcional) Agende como **Job** apontando para o notebook ou para `src/main.py`.

## Saídas

- `output/delta_seismic_gold/` — tabela Delta (`_delta_log/` + `event_date=YYYY-MM-DD/*.snappy.parquet`)
- `output/dq_report.json` — resultado de cada checagem de qualidade
