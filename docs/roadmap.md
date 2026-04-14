# Roadmap: completed work and enrichment directions

This document tracks **what the finance demo platform already delivers** and **planned themes** (realtime sources, dictionary/metrics for business users, data contracts and validation). Status values: **Done**, **In progress**, **Planned**.

Related docs: [data-product-design-overview.md](data-product-design-overview.md), [technical-design.md](technical-design.md), [data-design-and-flow.md](data-design-and-flow.md), [glossary.md](glossary.md), [catalog.md](catalog.md), [realtime-and-cdc.md](realtime-and-cdc.md), [bi-setup-and-semantic-alignment.md](bi-setup-and-semantic-alignment.md).

---

## Phase 0 — Baseline MVP (Done)

| Layer | Delivered |
| ----- | --------- |
| Sources and infra | Two operational Postgres databases (lending + insurance), one `analytics_db`; Docker Compose; Python synthetic generation and load. |
| Ingestion | Batch `make seed-data` pulling from source DBs into `analytics_db.staging` landing tables (no CDC in the default path). |
| Transform | dbt: staging → intermediate → marts; macros (`surrogate_key`, schema naming). |
| Business modeling | Deterministic identity resolution (`int_customer_identity_resolution` → `int_customer_360` → `dim_customer`); facts (loan, repayment, policy, claim); `dim_branch`, `dim_date`; `mart_branch_monthly_performance` (branch × month grain). |
| Quality and light contracts | dbt tests on sources and marts (`unique`, `not_null`, `relationships`, `accepted_values`); exposures for a conceptual dashboard. |
| Observability and docs | dbt docs, Mermaid lineage export under `lineage/`; design docs in `docs/`. |

**Known limitations (product-facing):** the baseline path is still manual `make` targets; **optional** local orchestration exists (Airflow Compose profile, Prefect flow). SCD2 applies to key dimensions (`dim_customer`, `dim_branch`); identity rules remain simple; insurance branch KPIs depend on resolution to lending; freshness is “as of last successful load + transform” unless Airflow (or another scheduler) is enabled.

---

## Theme A — Realtime and streaming sources (Planned / foundations started)

**Goal:** Move from batch-only reloads toward CDC, event streams, or scheduled micro-batch with lower latency.

| Item | Status | Notes |
| ---- | ------ | ----- |
| Document CDC and streaming options | Done | See [realtime-and-cdc.md](realtime-and-cdc.md). |
| Postgres `wal_level=logical` on source DBs (Compose) | Done | Enables logical decoding for future Debezium or native replication. |
| Example incremental staging on `staging.lending_loans` | Done | `stg_lending_loans` materialized as incremental table with `loaded_at` watermark (see model config). |
| Debezium / Kafka / full streaming stack | Planned | Optional extension; not bundled by default. |
| Orchestrated refresh (Airflow) | Done (optional) | Compose profile `airflow`; DAG under `airflow/dags/`; `make airflow-up` / `make airflow-dag-test`; see `orchestration/README.md`. |
| Orchestrated refresh (Prefect) | Done (optional) | `orchestration/refresh_flow.py` + `requirements-orchestration.txt`; run locally or wire to a Prefect server. |

---

## Theme B — Dictionary and metrics definition (In progress)

**Goal:** Shared definitions for columns and KPIs so business and engineering align; enable self-serve exploration via BI and (optionally) a catalog.

| Item | Status | Notes |
| ---- | ------ | ----- |
| Business glossary | Done | [glossary.md](glossary.md). |
| dbt Semantic Layer (semantic model + metrics) on branch monthly mart | Done | `models/marts/_mart_branch_monthly_semantic.yml`. |
| Metabase (optional Compose service) | Done | `make bi-up`; connect to `analytics_db` on the internal network. |
| BI semantic alignment guide + SQL templates | Done | `docs/bi-setup-and-semantic-alignment.md` and `bi/sql/*`. |
| Semantic curated serving layer for BI (`semantic` schema + build/validate scripts) | Done | `bi/semantic/contract.yml`, `scripts/build_semantic_artifacts.py`, `scripts/validate_semantic_artifacts.py`. |
| Enterprise catalog (DataHub, OpenMetadata, dbt Cloud) | Planned | Pointers in [catalog.md](catalog.md). |

---

## Theme C — Data contracts, operations, validation (In progress)

**Goal:** Explicit schemas, automated validation in CI, and a path toward production-style monitoring.

| Item | Status | Notes |
| ---- | ------ | ----- |
| Explicit YAML contracts for staging landing tables | Done | Under `contracts/schemas/`; versioning fields per table. |
| Contract validation script (structure + alignment with dbt `sources`) | Done | `scripts/validate_data_contracts.py`; `make validate-contracts`. |
| CI: Docker Compose + seed + dbt test | Done | `.github/workflows/dbt.yml`. |
| Soda / Great Expectations / dbt constraints in warehouse | Planned | Extend CI or add optional checks when you adopt a tool. |

---

## Suggested priority order

```mermaid
flowchart LR
  done[Baseline_MVP]
  contracts[Explicit_contracts_CI]
  dict[Dictionary_metrics]
  rt[Realtime_CDC]
  done --> contracts
  contracts --> dict
  dict --> rt
```

1. **Phase 1:** Explicit contracts and CI tests (reduces risk before more sources).  
2. **Phase 2:** Glossary, semantic metrics, Metabase (consistent definitions for consumers).  
3. **Phase 3:** CDC/streaming, orchestration, incremental patterns at scale.

---

## How to run optional pieces

| Action | Command / location |
| ------ | ------------------ |
| Validate staging landing contracts | `make validate-contracts` |
| Start Metabase | `docker compose --profile bi up -d` (see `.env.example` for `METABASE_PORT`) |
| Airflow (after `make up` and `bash scripts/bootstrap.sh` for `profiles.yml`) | `make airflow-up`; UI and `make airflow-dag-test` — see `orchestration/README.md` |
| Prefect refresh flow (after `pip install -r requirements-orchestration.txt`) | `python orchestration/refresh_flow.py` or register with Prefect as needed |
