# Data lineage feature overview

This document explains **how lineage is exposed** in the demo, **what artifacts exist**, and **how to extend** lineage visibility without OpenMetadata or DataHub.

## 1. Goals

- Show **which dbt models** depend on which **sources and refs**.
- Keep tooling **lightweight** and **versionable** (text / HTML / Mermaid).
- Avoid running a separate metadata service for the minimal stack.

## 2. Lineage surfaces

### 2.1 dbt project graph (primary)

**Tool:** `dbt docs generate` + `dbt docs serve`.

**Artifacts:** Written under `dbt_project/target/`, including `manifest.json`, `catalog.json`, and static doc site assets.

**What you see:** Interactive DAG: `source` nodes → `staging` → `intermediate` → `marts`, with column-level metadata where documented.

**When to use:** Deep dives, onboarding, and explaining transformations model-by-model.

### 2.2 Exported Mermaid graph (secondary)

**Tool:** `lineage/render_lineage.py`

**Input:** `dbt_project/target/manifest.json` (run `make docs` first).

**Outputs:**

- `lineage/lineage.mmd` — raw Mermaid `flowchart LR`
- `lineage/lineage.md` — Markdown wrapper with a fenced `mermaid` block for GitHub / GitLab / Confluence-style renderers

**Logic:** Enumerate all `resource_type == "model"` nodes in the manifest; for each model, add directed edges from **model** dependencies (other models only—sources appear as upstream deps in dbt’s graph but manifest `depends_on.nodes` includes both sources and models; the script filters to model→model edges for a compact graph).

**Note:** To include **sources** as named nodes, extend the script to emit edges from `source.*` nodes (manifest lists them with `resource_type == "source"` or as parents in `depends_on`). The current script optimizes for a **compact model-only** DAG.

### 2.3 Documentation in repo

- `README.md` — Source-to-mart table and architecture diagram.
- `docs/data-design-and-flow.md` — Layering and ER-style context.
- `models/sources.yml` — Explicit physical source tables and tests (contract + discovery).

## 3. How to run lineage export

```bash
make docs      # produces manifest.json
make lineage   # writes lineage/lineage.mmd and lineage/lineage.md
```

Preview Mermaid in any compatible viewer, or paste `lineage.mmd` into [Mermaid Live Editor](https://mermaid.live).

## 4. Relationship to tests and exposures

- **Tests** in `sources.yml` and `_marts__models.yml` document **expected keys and FKs**; they complement lineage by stating **integrity rules** on edges.
- **Exposures** (`models/exposures.yml`) document **downstream usage** (e.g., a conceptual dashboard) and link the mart to a consumer in the dbt DAG.

## 5. What this stack does not provide

- Automated column-level lineage into BI tools.
- **Automatic** unified graphs (e.g., catalog UI linking Airflow tasks to dbt models) without deploying catalog ingestion for both systems.
- Persistent catalog with RBAC, sampling, or PII classification.

**Note:** Optional Airflow ([`airflow/dags/finance_demo_daily.py`](../airflow/dags/finance_demo_daily.py)) makes the batch steps explicit (`ingest_sources_to_staging` → landing tables in `sources.yml` → `dbt_run_and_test`); use that as operational context until you connect a catalog to Airflow and dbt metadata.

## 6. Extension patterns

| Need | Approach |
|------|----------|
| Source nodes in Mermaid | Extend `render_lineage.py` to parse `sources` from manifest and draw `source → model` edges |
| Machine-readable lineage | Use `manifest.json` / `catalog.json` directly in CI; JSON Schema optional |
| Enterprise catalog | Export manifest to DataHub/OpenMetadata later; same dbt artifacts are the integration point |
| Git-tracked snapshot | Commit `lineage.md` after releases (team policy); or attach as CI artifact |

## 7. Troubleshooting

| Symptom | Likely cause |
|---------|----------------|
| `manifest not found` | Run `make docs` before `make lineage` |
| Empty or tiny graph | Project not compiled; check `dbt_project/target/manifest.json` size and `nodes` count |
| Mermaid parse error | Special characters in model names; adjust sanitization in `render_lineage.py` |
