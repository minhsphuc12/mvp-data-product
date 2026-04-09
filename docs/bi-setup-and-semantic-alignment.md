# BI setup and semantic alignment

This guide makes BI reporting reproducible locally and keeps report numbers aligned with semantic metrics in `dbt_project/models/marts/_mart_branch_monthly_semantic.yml`.

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

## 4) Build questions with semantic-consistent formulas

Use SQL templates from:

- `bi/sql/metabase_branch_monthly_kpi_base.sql`
- `bi/sql/metabase_reports.sql`

Important alignment rule:

- Always compute ratio KPIs from **aggregated totals** at the selected grain.
- Example (correct): `sum(repayment_amount) / nullif(sum(loan_disbursement_amount), 0)`
- Avoid averaging pre-computed row-level ratios unless you intentionally need unweighted behavior.

## 5) Suggested dashboard tiles

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

## 6) Validation checklist before sharing reports

1. Numbers reconcile with mart totals using direct SQL on `marts.mart_branch_monthly_performance`.
2. Ratios use null-safe denominator (`nullif(..., 0)`).
3. Date filter is at month grain (`month_start_date`).
4. Branch filter uses `branch_id`.

## 7) Mapping to semantic metrics

| Semantic metric | SQL expression on mart |
| --- | --- |
| `total_loan_disbursement_amount` | `sum(loan_disbursement_amount)` |
| `total_repayment_amount` | `sum(repayment_amount)` |
| `total_active_policies` | `sum(active_policy_count)` |
| `total_number_of_loans` | `sum(number_of_loans)` |
| `total_claim_amount` | `sum(claim_amount)` |
| `total_cross_sell_customers` | `sum(cross_sell_customer_count)` |
| `repayment_coverage_ratio` | `sum(repayment_amount) / nullif(sum(loan_disbursement_amount), 0)` |
| `claim_to_disbursement_ratio` | `sum(claim_amount) / nullif(sum(loan_disbursement_amount), 0)` |
| `avg_disbursement_per_loan` | `sum(loan_disbursement_amount) / nullif(sum(number_of_loans), 0)` |
| `cross_sell_per_loan_ratio` | `sum(cross_sell_customer_count) / nullif(sum(number_of_loans), 0)` |
