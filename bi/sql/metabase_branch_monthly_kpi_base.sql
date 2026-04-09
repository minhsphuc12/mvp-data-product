-- Base query for Metabase model "branch_monthly_kpi_base".
-- This SQL mirrors semantic metric formulas defined in:
-- dbt_project/models/marts/_mart_branch_monthly_semantic.yml

with base as (
    select
        month_start_date,
        branch_id,
        loan_disbursement_amount,
        repayment_amount,
        active_policy_count,
        number_of_loans,
        claim_amount,
        cross_sell_customer_count
    from marts.mart_branch_monthly_performance
)
select
    month_start_date,
    branch_id,
    loan_disbursement_amount as total_loan_disbursement_amount,
    repayment_amount as total_repayment_amount,
    active_policy_count as total_active_policies,
    number_of_loans as total_number_of_loans,
    claim_amount as total_claim_amount,
    cross_sell_customer_count as total_cross_sell_customers,
    repayment_amount / nullif(loan_disbursement_amount, 0) as repayment_coverage_ratio,
    claim_amount / nullif(loan_disbursement_amount, 0) as claim_to_disbursement_ratio,
    loan_disbursement_amount / nullif(number_of_loans, 0) as avg_disbursement_per_loan,
    cross_sell_customer_count / nullif(number_of_loans, 0)::numeric as cross_sell_per_loan_ratio
from base;
