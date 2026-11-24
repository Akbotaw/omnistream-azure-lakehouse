# OmniStream Azure Lakehouse: Real-Time Airbnb CDC & Medallion Data Platform

![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/Apache_Spark-3.5.3-E25A1C?logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.2.1-00ADD8?logo=deltalake&logoColor=white)
![Databricks](https://img.shields.io/badge/Azure_Databricks-Unity_Catalog-FF3621?logo=databricks&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache_Kafka-3.8_KRaft-231F20?logo=apachekafka&logoColor=white)
![Debezium](https://img.shields.io/badge/Debezium-Log--Based_CDC-00A1E0)
![dbt](https://img.shields.io/badge/dbt-Gold_Marts-FF694B?logo=dbt&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache_Airflow-Orchestration-017CEE?logo=apacheairflow&logoColor=white)
![Tests](https://img.shields.io/badge/Tests-pytest_%2B_chispa-0A9EDC?logo=pytest&logoColor=white)

An end-to-end, production-style **Data Lakehouse** built on **Azure & Databricks**. This platform captures real-time transactional changes (`INSERT`, `UPDATE`, `DELETE`) from an operational **Airbnb PostgreSQL OLTP database** via **Write-Ahead Log (WAL) Change Data Capture (CDC)** using **Debezium** and **Apache Kafka (KRaft) / Azure Event Hubs**, processes them through a **PySpark & Delta Lake Medallion Architecture (`Bronze ➔ Silver`)**, transforms analytical dimensional models in **`dbt` (`Silver ➔ Gold`)**, and integrates **Apache Airflow**, **Unity Catalog**, and **Model Context Protocol (MCP)** AI diagnostics.

---

## System Architecture

```mermaid
flowchart LR
    subgraph Source["1. Transactional Source & Log-Based CDC"]
        PG[("PostgreSQL 16\n(wal_level=logical\nREPLICA IDENTITY FULL)")] -->|pgoutput WAL Slot| DBZ["Debezium Server 2.6"]
        DBZ -->|JSON CDC Envelopes| KAFKA[["Apache Kafka 3.8 (KRaft)\n/ Azure Event Hubs"]]
    end

    subgraph Lakehouse["2. Azure Databricks & Delta Lake (Unity Catalog + ADLS Gen2)"]
        KAFKA -->|PySpark Structured Streaming\ntrigger(availableNow=True)| BRONZE[("Bronze Layer\nImmutable Raw CDC Payloads")]
        BRONZE -->|LSN Deduplication, Quarantine,\nDelta MERGE & SCD Type 2| SILVER[("Silver Layer\nlistings (SCD2) | bookings (SCD1)\npayments (SCD1) | _quarantine")]
        SILVER -->|dbt Core / dbt-databricks\n+ Currency Normalization| GOLD[("Gold Layer (Star Schema)\ndim_listings | fct_bookings\nmart_city_revenue")]
    end

    subgraph Ops["3. Orchestration, Quality & AI Tooling"]
        AIRFLOW["Apache Airflow\n(Datasets, Dynamic Tasks, Deferrable Ops)"] --> BRONZE
        AIRFLOW --> SILVER
        AIRFLOW --> GOLD
        PYTEST["Automated Testing\n(pytest + chispa + dbt test)"] --> SILVER
        PYTEST --> GOLD
        MCP["FastMCP Lakehouse Server\n& AI Edge-Case Generator"] <--> GOLD
    end
```

---

## Key Engineering Challenges Solved

1. **Zero-Impact Log-Based CDC (`PostgreSQL WAL ➔ Debezium ➔ Kafka KRaft`)**:
   - Configured PostgreSQL logical replication (`wal_level = logical`, `pgoutput` replication slot) with `REPLICA IDENTITY FULL` across `airbnb.listings`, `airbnb.bookings`, and `airbnb.payments` to capture complete `before` and `after` row states.
   - Configured `debezium.source.decimal.handling.mode=double` to prevent Base64 binary encoding of `NUMERIC(10,2)` financial columns.
2. **Idempotent PySpark Structured Streaming (`Bronze Layer`)**:
   - Ingests Kafka streams using `.trigger(availableNow=True)` and durable Delta checkpoints (`checkpointLocation`) for cost-efficient, exactly-once incremental micro-batch processing without running 24/7 idle clusters.
3. **CDC Edge-Case Handling (`Silver Layer`)**:
   - **Hard Delete Key Preservation (`op = 'd'`)**: Extracts primary keys and attributes via `F.coalesce(F.col("cdc.after.<col>"), F.col("cdc.before.<col>"))` so deleted records (where `after` is `null`) are accurately propagated instead of silently dropped.
   - **In-Batch LSN Deduplication**: Resolves Delta Lake `MULTIPLE_SOURCE_ROW_MATCHING` errors using `Window.partitionBy(pk).orderBy(F.col("lsn").desc(), F.col("ts_ms").desc())` before executing `DeltaTable.merge()`.
   - **Out-of-Order Event Protection**: Enforces `source.lsn >= target.lsn` conditions inside `whenMatchedUpdateAll` and `whenMatchedDelete` so late-arriving network events never overwrite newer state.
   - **SCD Type 2 Historical Price Tracking**: Deduplicates technical network retries by `(listing_id, lsn)` while preserving all distinct historical price changes on `airbnb.listings`, computing `valid_from` and `valid_to` intervals via `F.lead()`.
   - **Dead-Letter Quarantine (`_quarantine`)**: Routes malformed JSON payloads or null-PK records to an isolated Delta quarantine table with raw payload lineage instead of failing the pipeline.
4. **Dimensional Modeling & Data Quality Gates (`dbt Gold Layer`)**:
   - Decodes Debezium Unix epoch-day dates (`DATE '1970-01-01' + CAST(check_in_date AS INTEGER)`) in zero-copy `staging` views (`materialized='view'`).
   - Builds Gold Star Schema tables (`dim_listings`, `fct_bookings`, `mart_city_revenue`) normalizing multi-currency transactions (`USD`, `GBP`, `EUR`, `KZT`) into `USD` via `dbt seed` exchange rates.
   - Enforces 11 automated schema and referential integrity tests (`unique`, `not_null`, `accepted_values`, `relationships`).
5. **Fast Local PySpark Unit Testing (`pytest` + `chispa`)**:
   - Decouples pure DataFrame transformation functions from I/O, enabling deterministic local unit tests with session-scoped Spark fixtures (`conftest.py`) that execute in `<4 seconds`.

---

## Repository Structure

```text
omnistream-azure-lakehouse/
├── .github/workflows/          # CI/CD pipelines: ruff, pytest, chispa, dbt test, DABs deploy (planned)
├── cdc_simulator/              # Local Docker CDC stack
│   ├── docker-compose.yml      # PostgreSQL 16 (logical WAL) + Kafka 3.8 (KRaft) + Debezium 2.6 + Kafka UI
│   ├── postgres/init.sql       # Airbnb OLTP schema (listings, bookings, payments) & seed data
│   └── debezium/               # Debezium Server configuration (pgoutput -> Kafka)
├── src/                        # PySpark & Delta Lake Medallion pipelines
│   ├── utils/spark_session.py  # Hybrid SparkSession factory (Local Delta/Kafka jars vs Azure Databricks)
│   ├── bronze/ingest_cdc.py    # Kafka -> Bronze Delta Lake Structured Streaming ingestion
│   └── silver/cdc_processor.py # Bronze -> Silver CDC parser, quarantine, SCD1 MERGE & SCD2 processor
├── dbt_lakehouse/              # dbt project (Silver -> Gold analytical marts)
│   ├── dbt_project.yml         # Project configuration
│   ├── profiles.yml            # Dual target profile (Local DuckDB Delta scan + Cloud Azure Databricks)
│   ├── seeds/                  # Multi-currency exchange rates (USD, GBP, EUR, KZT)
│   └── models/
│       ├── staging/            # stg_listings, stg_bookings, stg_payments (views)
│       └── marts/              # dim_listings, fct_bookings, mart_city_revenue + _marts_schema.yml tests
├── airflow/                    # Apache Airflow DAGs: Datasets, Dynamic Task Mapping, Deferrable ops (in progress)
├── ai_mcp/                     # FastMCP server for Lakehouse diagnostics & Pydantic AI test generation (planned)
├── infra/                      # Terraform modules for Azure: ADLS Gen2, Event Hubs, Unity Catalog (planned)
├── tests/                      # Automated test suite
│   ├── conftest.py             # Session-scoped PySpark fixture with teardown
│   └── unit/                   # test_bronze.py & test_silver.py (chispa DataFrame assertions)
├── databricks.yml              # Databricks Asset Bundles (DABs) deployment definition (planned)
└── pyproject.toml              # Python 3.11 packaging & dependencies (setuptools)
```

---

## Project Roadmap & Implementation Status

- [x] **Phase 1: Monorepo Architecture & Packaging** (`pyproject.toml`, editable package install, Git workflow)
- [x] **Phase 2: Log-Based CDC Infrastructure** (Dockerized PostgreSQL 16 WAL, Debezium Server, Apache Kafka 3.8 KRaft, Redpanda Console UI)
- [x] **Phase 3: PySpark & Delta Lake Medallion (`Bronze & Silver`)** (Structured Streaming, LSN window deduplication, `COALESCE` delete handling, SCD Type 2 history, Dead-Letter Quarantine)
- [x] **Phase 4: Unit Testing Framework** (`pytest` + `chispa` covering all 5 CDC edge cases)
- [x] **Phase 5: `dbt` Gold Transformations & Data Quality** (Staging views, multi-currency Gold marts, 11 schema & referential integrity tests, lineage docs)
- [ ] **Phase 6: Advanced Apache Airflow Orchestration** *(In Progress — Data-Aware Scheduling via Datasets, Dynamic Task Mapping `.expand()`, Deferrable Databricks operators)*
- [ ] **Phase 7: AI Integration & MCP Server** *(Planned — Custom `FastMCP` server for Delta table health/lineage & LLM-driven adversarial CDC test generation)*
- [ ] **Phase 8: Azure Terraform, Unity Catalog & CI/CD** *(Planned — ADLS Gen2, Managed Identity, Unity Catalog governance, Databricks Asset Bundles & GitHub Actions)*

---

## Quickstart: Run Locally

### 1. Start the Log-Based CDC Stack (PostgreSQL + Debezium + Kafka KRaft + Kafka UI)
```bash
docker compose -f cdc_simulator/docker-compose.yml up -d
# Inspect live CDC topics at http://localhost:8080/topics
```

### 2. Install Python Environment & Run Unit Tests (`pytest` + `chispa`)
```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pip install dbt-core dbt-databricks dbt-duckdb

pytest -v
```

### 3. Run PySpark Medallion Pipelines (`Kafka ➔ Bronze ➔ Silver`)
```bash
python -m src.bronze.ingest_cdc
python -m src.silver.cdc_processor
```

### 4. Build & Test `dbt` Gold Analytical Marts (`Silver ➔ Gold`)
```bash
dbt seed --project-dir dbt_lakehouse --profiles-dir dbt_lakehouse
dbt run --project-dir dbt_lakehouse --profiles-dir dbt_lakehouse
dbt test --project-dir dbt_lakehouse --profiles-dir dbt_lakehouse
```