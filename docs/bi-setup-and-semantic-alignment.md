# BI setup and semantic alignment

This guide makes BI reporting reproducible locally and keeps report numbers aligned with semantic metrics in `dbt_project/models/marts/_mart_branch_monthly_semantic.yml`.
It also enforces a semantic-first consumption path so business users do not write SQL against marts tables directly.

## 1) Start the stack and build the mart

```bash
make up
make seed-data
make transform
```

At this point, `marts.mart_branch_monthly_performance` is ready in `analytics_db`.

## 2) Start Metabase (optional BI profile)

```bash
make bi-up
```

Open Metabase at `http://localhost:${METABASE_PORT}` (default `3000`).

To stop BI services later:

```bash
make bi-down
```

If port `3000` is already used on your machine, set another port in `.env`:

```bash
METABASE_PORT=3001
```

## 3) Connect Metabase to analytics_db

In Metabase:

1. Admin -> Databases -> Add database -> PostgreSQL
2. Use these values on the Docker network:
   - Host: `analytics_db`
   - Port: `5432`
   - Database: `analytics_db`
   - Username/Password: values from `.env` (`ANALYTICS_DB_USER`, `ANALYTICS_DB_PASSWORD`)
3. Save and trigger a schema sync.

## 4) Build semantic curated artifacts

Generate curated semantic views in `analytics_db`:

```bash
make semantic-build
```

Validate reconciliation against source mart totals:

```bash
make semantic-validate
```

Contract location:

- `bi/semantic/contract.yml`

Generated artifacts (schema `semantic`):

- `semantic.branch_monthly_kpi_base`
- `semantic.executive_monthly_trend`
- `semantic.branch_risk_watchlist`

## 5) Build questions using semantic schema only

Use SQL templates from:

- `bi/sql/metabase_branch_monthly_kpi_base.sql`
- `bi/sql/metabase_reports.sql`

Important alignment rule:

- Semantic curated views already enforce KPI formulas from the semantic contract.
- If admin users still create native SQL questions, point only to `semantic.*` objects.
- Business users should use GUI questions and dashboards on top of semantic models.

## 6) Restrict business access (no native SQL on marts)

1. Run grants:

```sql
-- Execute file: bi/sql/metabase_business_permissions.sql
```

Create/update a real login user and attach it to `bi_business_readonly`:

```bash
psql "$ANALYTICS_DATABASE_URL" \
  -v metabase_bi_user='metabase_business' \
  -v metabase_bi_password='change_me_strong_password' \
  -f bi/sql/bootstrap_metabase_business_user.sql
```

2. In Metabase:
   - Create groups: `Business`, `Analyst`, `Admin`.
   - Disable Native query editor for `Business`.
   - Set table permissions for `Business` to allow only `semantic` schema.
3. Publish curated questions/dashboards in shared collections and give `Business` read access only.

## 7) Suggested dashboard tiles

1. **Executive trend (monthly):**
   - `total_loan_disbursement_amount`
   - `total_repayment_amount`
   - `repayment_coverage_ratio`
   - `claim_to_disbursement_ratio`
2. **Branch leaderboard (single month):**
   - Rank by `total_loan_disbursement_amount`
   - Show `avg_disbursement_per_loan` and `cross_sell_per_loan_ratio`
3. **Risk watchlist:**
   - Highest `claim_to_disbursement_ratio`
   - Keep `loan_disbursement_amount > 0` filter

## 8) Validation checklist before sharing reports

1. `make semantic-validate` passes.
2. Ratios use null-safe denominator (`nullif(..., 0)`).
3. Date filter is at month grain (`month_start_date`).
4. Branch filter uses `branch_id`.
5. Business group has no native SQL access in Metabase.

## 9) Mapping to semantic metrics

| Semantic metric | Curated semantic output |
| --- | --- |
| `total_loan_disbursement_amount` | `semantic.branch_monthly_kpi_base.total_loan_disbursement_amount` |
| `total_repayment_amount` | `semantic.branch_monthly_kpi_base.total_repayment_amount` |
| `total_active_policies` | `semantic.branch_monthly_kpi_base.total_active_policies` |
| `total_number_of_loans` | `semantic.branch_monthly_kpi_base.total_number_of_loans` |
| `total_claim_amount` | `semantic.branch_monthly_kpi_base.total_claim_amount` |
| `total_cross_sell_customers` | `semantic.branch_monthly_kpi_base.total_cross_sell_customers` |
| `repayment_coverage_ratio` | `semantic.branch_monthly_kpi_base.repayment_coverage_ratio` |
| `claim_to_disbursement_ratio` | `semantic.branch_monthly_kpi_base.claim_to_disbursement_ratio` |
| `avg_disbursement_per_loan` | `semantic.branch_monthly_kpi_base.avg_disbursement_per_loan` |
| `cross_sell_per_loan_ratio` | `semantic.branch_monthly_kpi_base.cross_sell_per_loan_ratio` |
