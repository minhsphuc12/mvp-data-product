# Business glossary (finance demo)

Short definitions for conformed entities and key fields. Authoritative mart measures are summarized in [data-product-design-overview.md](data-product-design-overview.md) section 3; semantic metrics live in `dbt_project/models/marts/_mart_branch_monthly_semantic.yml`.

## Conformed entities

| Term | Meaning |
| ---- | ------- |
| **Customer (master)** | A person unified across lending and insurance via deterministic identity rules (`int_customer_identity_resolution`). |
| **customer_key** | Surrogate key in marts; equals `master_customer_id` from identity resolution. |
| **Branch** | Lending branch; system of record for branch attributes (`dim_branch`). |
| **branch_key** | Surrogate key for branch in facts (from `dim_branch`). |
| **Loan disbursement fact** | One row per loan at disbursement (`fct_loan_disbursement`). |
| **Repayment fact** | One row per repayment event (`fct_repayment`). |
| **Policy fact** | One row per insurance policy (`fct_policy`). |
| **Claim fact** | One row per insurance claim (`fct_claim`). |

## Mart grain and keys

| Term | Meaning |
| ---- | ------- |
| **mart_branch_monthly_performance** | One row per `(branch_id, month_start_date)`. |
| **branch_month_sk** | MD5 hash surrogate for the branch-month grain (unique in mart). |
| **month_start_date** | Calendar month start date for the rollup. |

## KPIs (branch × month)

| Measure | Plain-language definition |
| ------- | --------------------------- |
| **loan_disbursement_amount** | Total principal of loans disbursed in the branch in that month. |
| **number_of_loans** | Count of loans disbursed in the branch-month. |
| **repayment_amount** | Sum of **completed** repayments (from staging loans + repayments) in the branch-month. |
| **active_policy_count** | Distinct active policies with coverage overlapping any day in the month, holder resolved to lending with matching `primary_branch_id`. |
| **claim_amount** | Sum of claim amounts filed in the month (same branch attribution as active policies). |
| **cross_sell_customer_count** | Distinct customers with a loan disbursement and an active policy in the same branch-month. |

## Attribution caveats

- **Insurance-only customers** (no match to lending) are **excluded** from branch-level insurance KPIs by design in this demo.
- **Identity** is rule-based (national ID, then phone + normalized name), not probabilistic or manually curated.

## Semantic layer metrics (dbt)

| Metric name | Maps to |
| ----------- | ------- |
| `total_loan_disbursement_amount` | Sum of `loan_disbursement_amount` over selected branch-months. |
| `total_repayment_amount` | Sum of `repayment_amount`. |
| `total_active_policies` | Sum of `active_policy_count` (interpret at branch × month grain). |

Use `dbt sl query` (where available) or downstream BI on `mart_branch_monthly_performance` for exploration.
