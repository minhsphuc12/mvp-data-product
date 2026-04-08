# Technical design

This document describes the **local demo** finance data platform: components, runtime topology, configuration, and operational boundaries.

## 1. Scope and goals

- **In scope:** Two operational Postgres databases, one analytics Postgres, batch synthetic load, dbt transformations, local lineage artifacts, optional Metabase (Compose profile `bi`), explicit raw YAML contracts, GitHub Actions CI, optional Prefect flow, incremental staging example.
- **Out of scope (by default):** Full streaming stack (Kafka, Debezium), production HA, hosted catalog deployment. See [roadmap.md](roadmap.md) and [realtime-and-cdc.md](realtime-and-cdc.md) for extension paths.

**Primary goal:** Make **data origin and transformations inspectable** in under 30 minutes for a single engineer.

## 2. Runtime architecture

| Component | Technology | Role |
|-----------|------------|------|
| Orchestration | Docker Compose | Single command to start three Postgres instances |
| Source systems | PostgreSQL 16 (Alpine) | `source_db_1` (lending), `source_db_2` (insurance) |
| Warehouse host | PostgreSQL 16 | `analytics_db` holds raw mirrors and dbt-built schemas |
| Data generation & load | Python 3.11–3.13 | Generates deterministic synthetic data; truncates sources; loads sources + `raw_*` mirror |
| Transformations | dbt-core + dbt-postgres | Staging → intermediate → marts on `analytics_db` only |
| Lineage export | Python script | Reads `target/manifest.json`, emits Mermaid |

Ports and credentials are **environment-driven** via `.env` (see `.env.example`).

## 3. Data movement pattern

1. **Operational truth (simulated):** Lending and insurance apps write to their respective source DBs (here: populated only by the demo loader).
2. **Analytics landing:** The same payloads are **re-inserted** into `analytics_db.raw_lending` and `analytics_db.raw_insurance` with a `loaded_at` column where defined.
3. **Transformations:** dbt reads **only** `raw_*` schemas; no cross-database dbt connections.

This is **batch replication**, not log-based CDC.

## 4. Schema layout on `analytics_db`

| Schema | Owner / tool | Contents |
|--------|----------------|----------|
| `raw_lending`, `raw_insurance` | Python loader | Mirrored operational tables |
| `staging` | dbt | Mostly `stg_*` views; `stg_lending_loans` is an incremental table (merge on `loan_id`) for CDC-style demos |
| `intermediate` | dbt | Identity resolution, 360 view, loan cashflow, policy-claim rollups |
| `marts` | dbt (tables) | Dimensions, facts, `mart_branch_monthly_performance` |

Custom macro `generate_schema_name` maps dbt `+schema` values to **exact** Postgres schema names (no `target_` prefix).

## 5. Application layout (repository)

- `infra/*_init/` — DDL executed on first container startup.
- `data_gen/` — Synthetic generation and `load_data` entrypoint.
- `dbt_project/` — dbt project root; `profiles.yml` is gitignored; `profiles.yml.example` is committed.
- `lineage/render_lineage.py` — Post-`dbt docs generate` Mermaid export.
- `scripts/bootstrap.sh` — venv, pip, profile copy.

## 6. Configuration and secrets

- **Never commit** `dbt_project/profiles.yml` with real production secrets; use `.env` + example profile.
- dbt is invoked with `DBT_PROFILES_DIR` pointing at `dbt_project/` so the project is self-contained.

## 7. Build and run interface

| Command | Effect |
|---------|--------|
| `make up` | Start Compose stack |
| `make seed-data` | Truncate + load sources + raw mirror |
| `make transform` | `dbt run` |
| `make test` | `dbt test` |
| `make docs` | `dbt docs generate` |
| `make lineage` | Manifest → `lineage/lineage.mmd` + `lineage/lineage.md` |
| `make demo` | `up` + short wait + seed + dbt + tests + docs + lineage |

## 8. Non-functional choices

- **Determinism:** Fixed RNG seeds in generators for reproducible demos.
- **Python version:** dbt-core is validated on 3.11–3.13; 3.14+ may fail (see `requirements.txt` / `bootstrap.sh`).
- **Performance:** No partitioning, no incremental models required for demo volumes.

## 9. Extension hooks (not implemented)

- Optional Metabase service in Compose.
- SCD2 on `dim_customer`.
- Orchestrated schedules (cron, Prefect, Airflow).
- Real-time ingestion or external ELT tool reading binlogs/WAL.
