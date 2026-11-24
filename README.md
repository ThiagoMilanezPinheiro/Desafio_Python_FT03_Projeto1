# Seismic Pipeline — Databricks & Local

Pipeline ETL resiliente que cruza **eventos sísmicos** (USGS) com **dados socioeconômicos de países** (World Bank), transforma com **PySpark + Spark SQL**, aplica validações automatizadas de **Data Quality** e persiste a camada **Gold** em **Delta Lake** (Parquet + Snappy), particionada por **ano/mês**.

## Arquitetura

```
 USGS Earthquake API ─┐                         ┌─► silver_earthquakes ─┐
   (GeoJSON, sismologia)│   extract.py            │                        │   quality.py         load.py
                        ├─► rate limit + retry ──►│ bronze (from_json) ───►├─► gold ─► DQ checks ─► Delta/Snappy
 World Bank API ───────┘   + backoff + jitter    │   + Spark SQL          │   (JOIN +  + JSON     event_year=/
   (JSON paginado, socioeconômico)                └─► silver_countries ────┤   métricas) report    event_month=
                                                  └─► silver_population ───┘
```

## Fontes de dados (APIs públicas, sem chave)

| API | Domínio | Endpoint | Uso |
|---|---|---|---|
| **USGS Earthquake Catalog** | Sismologia | `GET https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime=&endtime=&minmagnitude=` | Eventos sísmicos (GeoJSON com arrays/structs aninhados). Limite de 20 mil eventos por chamada → extração em janelas de `EXTRACT_WINDOW_DAYS`. [Docs](https://earthquake.usgs.gov/fdsnws/event/1/) |
| **World Bank — Countries** | Socioeconômico | `GET https://api.worldbank.org/v2/country?format=json&per_page=500` | Cadastro de países: ISO3, região, nível de renda, capital (structs aninhados). [Docs](https://datahelpdesk.worldbank.org/knowledgebase/articles/898590) |
| **World Bank — Indicador SP.POP.TOTL** | Socioeconômico | `GET https://api.worldbank.org/v2/country/all/indicator/SP.POP.TOTL?format=json&date=2018:2024` | População total por país/ano (paginada). [Docs](https://datahelpdesk.worldbank.org/knowledgebase/articles/889392) |

## Resiliência HTTP (`src/extract.py`)

- **Rate limiting**: intervalo mínimo entre chamadas (`API_RATE_LIMIT_PER_SEC`).
- **Retentativas** com **exponential backoff + jitter** (`API_BACKOFF_FACTOR ** tentativa + rand(0,1)`), respeitando o header `Retry-After`.
- Repete apenas falhas transitórias (`429`, `5xx`, timeout, conexão, JSON inválido); erros `4xx` definitivos falham imediatamente com `ExtractionError`.
- Paginação automática da API do World Bank e validação do formato da resposta.
- Estatísticas de requisições, retentativas e falhas vão para o relatório de auditoria.

## Transformações (`src/transform.py`, em Spark SQL)

| # | Transformação | Como |
|---|---|---|
| 1 | **Desaninhamento** de arrays/JSON | `LATERAL VIEW EXPLODE(features)` + acesso a structs (`f.properties.mag`, `region.value`) |
| 2 | **Padronização de tipos e datas** | `timestamp_millis()`, `CAST`, `TRIM`, `LOWER`, `YEAR/MONTH` |
| 3 | **Tratamento de nulos** | descarte de eventos sem id/data/magnitude/coordenadas; `COALESCE`/`NULLIF` em campos descritivos |
| 4 | **Deduplicação** | `ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY updated_ts DESC)`; população: ano mais recente por país |
| 5 | **Cruzamento de fontes (JOIN)** | terremoto → país (por nome/alias) → população; range join com classes de magnitude |
| 6 | **Enriquecimento de métricas** | `energy_joules` (Gutenberg-Richter), `mag_class`, `depth_class`, `is_significant`, `country_month_events`, `events_per_million_pop` |

## Data Quality (`src/quality.py`)

Validações automatizadas sobre a camada Gold; se alguma falhar, **nada é gravado** e o pipeline termina com erro.

- **Schema**: contrato de colunas e tipos esperados.
- **Null ratio**: limite por coluna (0% para campos obrigatórios).
- **Unicidade**: `event_id`.
- **Intervalos**: latitude, longitude, magnitude, profundidade, mês e flag de tsunami.
- **Métricas de consistência** (auditoria): intervalo temporal, taxa de match de países, linhas por partição, top países, estatísticas HTTP e contagens por camada.

Saída: `output/dq_report.json`.

## Estrutura

| Caminho | Responsabilidade |
|---|---|
| `src/config.py` | Configurações: `dbutils.secrets` → `os.getenv`/`.env` → padrão |
| `src/extract.py` | Cliente HTTP resiliente + extratores USGS e World Bank |
| `src/transform.py` | Schemas explícitos, camadas bronze/silver/gold em Spark SQL |
| `src/quality.py` | Validações de qualidade e relatório JSON |
| `src/load.py` | SparkSession (local ou Databricks) e escrita Delta particionada |
| `src/main.py` | Entrypoint que orquestra o fluxo |
| `notebooks/run_seismic_etl.py` | Notebook Databricks que chama `src.main.run(spark)` |

## Variáveis de ambiente (`.env.example`)

| Variável | Padrão | Descrição |
|---|---|---|
| `USGS_API_URL` | `https://earthquake.usgs.gov/fdsnws/event/1/query` | Endpoint USGS |
| `WORLDBANK_API_URL` | `https://api.worldbank.org/v2` | Base da API World Bank |
| `API_TIMEOUT` | `30` | Timeout por requisição (s) |
| `API_MAX_RETRIES` | `5` | Tentativas máximas por requisição |
| `API_BACKOFF_FACTOR` | `2` | Base do backoff exponencial |
| `API_RATE_LIMIT_PER_SEC` | `2` | Requisições por segundo |
| `START_DATE` / `END_DATE` | `2024-01-01` / `2024-04-01` | Período (fim exclusivo) |
| `EXTRACT_WINDOW_DAYS` | `7` | Tamanho de cada janela de extração |
| `MIN_MAGNITUDE` | `2.5` | Magnitude mínima |
| `POPULATION_YEARS` | `2018:2024` | Anos consultados de população |
| `OUTPUT_PATH` | `output/delta_seismic_gold` | Destino da tabela Delta |
| `DQ_REPORT_PATH` | `output/dq_report.json` | Destino do relatório |
| `DATABRICKS_SECRET_SCOPE` | `seismic` | Scope de secrets no Databricks (opcional) |

As APIs usadas não exigem chave. Credenciais futuras devem ir no `.env` (ignorado pelo Git) ou num secret scope do Databricks — nunca no código.

## Como rodar no Databricks (ambiente principal)

1. **Workspace → Create → Git folder** com a URL deste repositório.
2. Abra `notebooks/run_seismic_etl`, conecte em **Serverless** e clique em **Run all**.
   O notebook cria `workspace.seismic.gold` (volume) se não existir.
3. Para versionar a saída: defina o widget `EXPORT_TO_REPO = yes`, rode a última célula e faça **Commit & Push** no painel Git.

## Como rodar localmente

Pré-requisitos: **Python 3.10 ou 3.11** (PySpark 3.5) e **Java 11/17**.

```bash
cp .env.example .env
./run.sh            # cria .venv, instala dependências e executa
./run.sh submit     # alternativa via spark-submit
```

No **Windows**, o Spark exige o `winutils.exe` do Hadoop: defina `HADOOP_HOME` apontando para uma pasta com `bin/winutils.exe` e `bin/hadoop.dll` (Hadoop 3.3.x), ou execute pelo **WSL**.

## Saídas

A sessão Spark é fixada em **UTC** (`spark.sql.session.timeZone`), então datas e partições não dependem do fuso da máquina: a execução local e a do Databricks produzem exatamente as mesmas partições. A escrita faz `repartition` pelas colunas de partição, gerando um arquivo Parquet por mês.

```
output/
├── delta_seismic_gold/
│   ├── _delta_log/                       # log transacional Delta
│   └── event_year=2024/event_month=1/    # partições ano/mês
│       └── part-*.snappy.parquet
└── dq_report.json                        # auditoria de qualidade
```
