# Data catalog options

This repo ships **dbt docs** (`make docs`) and a **Mermaid lineage** export (`make lineage`) as lightweight catalog and lineage views. **Optional Apache Airflow** (Compose profile `airflow`, see [technical-design.md](technical-design.md)) adds a scheduled DAG for load + dbt; in production, catalogs such as DataHub or OpenMetadata can ingest both dbt `manifest.json` and orchestrator metadata for cross-system lineage.

For teams that need searchable ownership, PII tags, and cross-project lineage, consider:

| Tool | Role |
| ---- | ---- |
| **dbt Cloud / dbt Explorer** | Native semantic layer + lineage for dbt projects. |
| **DataHub** | Open-source catalog; ingest dbt `manifest.json` and warehouse metadata. |
| **OpenMetadata** | Open-source catalog with profiling and glossary workflows. |

**Suggested path:** keep YAML descriptions and [glossary.md](glossary.md) in git; publish `target/manifest.json` and `catalog.json` from CI as artifacts for catalog ingestion.
