-- Report SQL snippets for Metabase (native SQL questions).
-- All KPI formulas match semantic definitions from:
-- dbt_project/models/marts/_mart_branch_monthly_semantic.yml

-- 1) Executive monthly trend (all branches)
select
    month_start_date,
    sum(loan_disbursement_amount) as total_loan_disbursement_amount,
    sum(repayment_amount) as total_repayment_amount,
    sum(claim_amount) as total_claim_amount,
    sum(cross_sell_customer_count) as total_cross_sell_customers,
    sum(repayment_amount) / nullif(sum(loan_disbursement_amount), 0) as repayment_coverage_ratio,
    sum(claim_amount) / nullif(sum(loan_disbursement_amount), 0) as claim_to_disbursement_ratio,
    sum(loan_disbursement_amount) / nullif(sum(number_of_loans), 0) as avg_disbursement_per_loan
from marts.mart_branch_monthly_performance
group by month_start_date
order by month_start_date;

-- 2) Branch ranking for a selected month
-- Replace :report_month with a value like '2024-06-01'.
select
    branch_id,
    loan_disbursement_amount as total_loan_disbursement_amount,
    repayment_amount as total_repayment_amount,
    claim_amount as total_claim_amount,
    cross_sell_customer_count as total_cross_sell_customers,
    repayment_amount / nullif(loan_disbursement_amount, 0) as repayment_coverage_ratio,
    claim_amount / nullif(loan_disbursement_amount, 0) as claim_to_disbursement_ratio,
    loan_disbursement_amount / nullif(number_of_loans, 0) as avg_disbursement_per_loan
from marts.mart_branch_monthly_performance
where month_start_date = :report_month
order by total_loan_disbursement_amount desc;

-- 3) Branch risk watchlist (highest claim ratio)
select
    month_start_date,
    branch_id,
    claim_amount as total_claim_amount,
    loan_disbursement_amount as total_loan_disbursement_amount,
    claim_amount / nullif(loan_disbursement_amount, 0) as claim_to_disbursement_ratio
from marts.mart_branch_monthly_performance
where loan_disbursement_amount > 0
order by claim_to_disbursement_ratio desc, total_claim_amount desc
limit 30;
