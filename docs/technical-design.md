# Technical design

This document describes the **local demo** finance data platform: components, runtime topology, configuration, and operational boundaries.

## 1. Scope and goals

- **In scope:** Two operational Postgres databases, one analytics Postgres, batch synthetic load, dbt transformations, local lineage artifacts, optional Metabase (Compose profile `bi`), optional Apache Airflow (Compose profile `airflow`) for scheduled end-to-end runs, explicit staging YAML contracts, GitHub Actions CI, optional Prefect flow, incremental staging example.
- **Out of scope (by default):** Full streaming stack (Kafka, Debezium), production HA, hosted catalog deployment. See [roadmap.md](roadmap.md) and [realtime-and-cdc.md](realtime-and-cdc.md) for extension paths.

**Primary goal:** Make **data origin and transformations inspectable** in under 30 minutes for a single engineer.

## 2. Runtime architecture

| Component | Technology | Role |
|-----------|------------|------|
| Runtime / infra | Docker Compose | Start three Postgres instances; optional profiles for Metabase (`bi`) and Airflow (`airflow`) |
| Batch orchestration (optional) | Apache Airflow 2.10.x | Daily-style DAG: Python load → SCD2 validation → `dbt run` + `dbt test` (see `airflow/dags/`); uses Compose service DNS for DB hosts |
| Source systems | PostgreSQL 16 (Alpine) | `source_db_1` (lending), `source_db_2` (insurance) |
| Warehouse host | PostgreSQL 16 | `analytics_db` holds staging landing tables and dbt-built schemas |
| Data generation & load | Python 3.11–3.13 | Generates deterministic synthetic data; truncates sources; loads sources and pulls snapshots into `analytics_db.staging` |
| Transformations | dbt-core + dbt-postgres | Staging -> intermediate -> marts on `analytics_db` only |
| Lineage export | Python script | Reads `target/manifest.json`, emits Mermaid |

Ports and credentials are **environment-driven** via `.env` (see `.env.example`). Airflow tasks inherit the same DB users/passwords as the Postgres services; **inside Compose**, loader and dbt use hostnames `source_db_1`, `source_db_2`, and `analytics_db` on port **5432** (not `localhost` and host-mapped ports). The loader sets `LOAD_DATA_AS_OF` to the DAG logical date (`{{ ds }}`) to mimic a business-day batch.

## 3. Data movement pattern

1. **Operational truth (simulated):** Lending and insurance apps write to their respective source DBs (here: populated only by the demo loader).
2. **Analytics landing:** Batch ingest pulls directly from production tables into `analytics_db.staging.*` with `loaded_at`.
3. **Transformations:** dbt reads only staging landing sources in `analytics_db`.

This is **batch replication**, not log-based CDC.

## 4. Schema layout on `analytics_db`

| Schema | Owner / tool | Contents |
|--------|----------------|----------|
| `staging` | Python loader + dbt sources | Landing tables (`lending_*`, `insurance_*`) used as dbt sources |
| `intermediate` | dbt | Identity resolution, 360 view, loan cashflow, policy-claim rollups |
| `marts` | dbt (tables) | Dimensions, facts, `mart_branch_monthly_performance` |

Custom macro `generate_schema_name` maps dbt `+schema` values to exact Postgres schema names (no `target_` prefix).

## 5. Application layout (repository)

- `infra/*_init/` — DDL executed on first container startup.
- `data_gen/` — Synthetic generation and `load_data` entrypoint.
- `dbt_project/` — dbt project root; `profiles.yml` is gitignored; `profiles.yml.example` is committed.
- `lineage/render_lineage.py` — Post-`dbt docs generate` Mermaid export.
- `airflow/` — Dockerfile (Airflow + dbt) and `dags/` for scheduled pipelines.
- `orchestration/refresh_flow.py` — Optional Prefect flow wrapping `make` targets.
- `scripts/bootstrap.sh` — venv, pip, profile copy.

## 6. Configuration and secrets

- **Never commit** `dbt_project/profiles.yml` with real production secrets; use `.env` + example profile.
- dbt is invoked with `DBT_PROFILES_DIR` pointing at `dbt_project/` so the project is self-contained.

## 7. Build and run interface

| Command | Effect |
|---------|--------|
| `make up` | Start Compose stack |
| `make bi-up` | Start optional Metabase service (`--profile bi`) |
| `make bi-down` | Stop optional Metabase service (`--profile bi`) |
| `make seed-data` | Truncate + load sources + pull into staging landing |
| `make transform` | `dbt run` |
| `make test` | `dbt test` |
| `make docs` | `dbt docs generate` |
| `make lineage` | Manifest -> `lineage/lineage.mmd` + `lineage/lineage.md` |
| `make demo` | `up` + short wait + seed + dbt + tests + docs + lineage |
| `make airflow-up` | Build/start Airflow metadata Postgres, init DB, scheduler, web UI (`--profile airflow`) |
| `make airflow-down` | Stop and remove Airflow profile containers (data volume: `airflow_meta_data`) |
| `make airflow-dag-test` | `airflow dags test finance_demo_daily <date>` inside the scheduler container |

See [orchestration/README.md](../orchestration/README.md) for UI URL, default admin user, and prerequisites (`dbt_project/profiles.yml` on the host for bind-mounted dbt).
