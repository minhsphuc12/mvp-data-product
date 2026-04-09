# Data product design overview

This document frames the demo as a **data product**: who it serves, what they get, definitions, limitations, and sensible next iterations.

## 1. Product intent

**Product name (working):** Local finance demo datamart.

**Audience:** Data engineers, analytics engineers, and technical stakeholders who need a **tangible** example of multi-source ingestion, identity resolution, and mart design **without** platform sprawl.

**Job to be done:** Answer, with SQL and lineage you can trace in minutes:

- Where did this metric come from?
- Which customers appear in both lending and insurance?
- How do branch-level KPIs relate to loans, repayments, policies, and claims?

## 2. Core entities (conformed)


| Entity    | Mart expression         | Notes                                                        |
| --------- | ----------------------- | ------------------------------------------------------------ |
| Customer  | `dim_customer`          | `customer_key` ties lending and insurance via resolution     |
| Branch    | `dim_branch`            | Sourced from lending (system of record for branch dimension) |
| Calendar  | `dim_date`              | Generated spine for joining facts to months and attributes   |
| Loan      | `fct_loan_disbursement` | Disbursement-centric fact                                    |
| Repayment | `fct_repayment`         | Cash movement events                                         |
| Policy    | `fct_policy`            | Contract-level fact                                          |
| Claim     | `fct_claim`             | Claim-level fact                                             |


## 3. Primary deliverable: branch × month mart

**Object:** `marts.mart_branch_monthly_performance`

**Grain:** One row per `(branch_id, month_start_date)`.

**Measures (as implemented):**


| Measure                     | Definition (summary)                                                                                                                                                   |
| --------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `loan_disbursement_amount`  | Sum of loan principal from `fct_loan_disbursement` in that branch-month                                                                                                |
| `number_of_loans`           | Count of loans disbursed in that branch-month                                                                                                                          |
| `repayment_amount`          | Sum of **completed** repayments (from staging loans + repayments) in that branch-month                                                                                 |
| `active_policy_count`       | Distinct policies with `policy_status = active`, coverage overlapping any day in the month, holder resolved to a lending customer with `primary_branch_id = branch_id` |
| `claim_amount`              | Sum of claim amounts filed in the month, same branch attribution rule                                                                                                  |
| `cross_sell_customer_count` | Distinct `master_customer_id` with a loan disbursement in the branch-month **and** an active policy overlapping that month, same branch                                |


**Grain key:** `branch_month_sk` (MD5 of branch + month) is tested for uniqueness.

## 4. User journeys

1. **Trust / lineage:** Engineer opens dbt docs DAG or `lineage/lineage.md` to see model dependencies.
2. **Quality:** Engineer runs `make test` for source and mart tests (`unique`, `not_null`, `relationships`, `accepted_values`).
3. **Analysis:** Analyst queries `mart_branch_monthly_performance` and drills to facts and dims.
4. **BI consumption:** Analyst starts Metabase via `make bi-up` and follows `docs/bi-setup-and-semantic-alignment.md` so report formulas stay aligned with semantic metrics.

## 5. SLAs and freshness (demo)

- **Freshness:** “As of last `make seed-data` + `make transform`.” There is no scheduler; reload is manual.
- **Availability:** Local Docker only; no HA or backup story in-repo.

## 6. Data contracts (implicit)

- **Sources:** Declared in `models/sources.yml` with column tests.
- **Marts:** Declared in `models/marts/_marts__models.yml` with key and relationship tests.
- **Exposures:** `models/exposures.yml` documents a conceptual dashboard dependency on the mart (no live BI in minimal stack).

## 7. Privacy and compliance

Synthetic data only; no real PII. National IDs and phones are random numeric patterns for demonstration.

## 8. Known limitations (product-facing)

- Identity resolution is **rule-based**, not probabilistic or curated.
- **No SCD2** on customer or branch.
- **Insurance-to-branch** mapping depends on resolution to lending; unmatched insurance-only customers do not appear in branch insurance KPIs.
- Volumes are small; performance patterns are not representative of production.

## 9. Roadmap ideas

**Canonical status table:** [roadmap.md](roadmap.md) (themes A/B/C: realtime, dictionary/metrics, data contracts).

| Theme       | Idea                                                                | Status (see roadmap)        |
| ----------- | ------------------------------------------------------------------- | --------------------------- |
| Consumption | Metabase (Compose profile `bi`) pointing at `analytics_db`          | Implemented (optional)      |
| Modeling    | SCD2 `dim_customer`, role-playing dates on facts                    | Planned                     |
| Metrics     | dbt Semantic Layer on `mart_branch_monthly_performance`             | Implemented                 |
| Dictionary  | Business glossary (`docs/glossary.md`)                              | Implemented                 |
| Operations  | CI dbt test, explicit raw contracts, Prefect example flow          | Partially implemented       |
| Realtime    | `wal_level=logical`, incremental staging example, CDC doc           | Foundations (see roadmap)   |
| Governance  | Catalog options (`docs/catalog.md`), lineage export to wiki         | Partially documented        |


