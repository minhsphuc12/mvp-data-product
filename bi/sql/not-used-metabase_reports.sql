-- Report SQL snippets for Metabase (native SQL questions).
-- All snippets read from semantic curated views (not marts raw table views).
-- Build/update semantic artifacts with: make semantic-build

-- 1) Executive monthly trend (all branches)
select
    month_start_date,
    total_loan_disbursement_amount,
    total_repayment_amount,
    total_claim_amount,
    total_cross_sell_customers,
    repayment_coverage_ratio,
    claim_to_disbursement_ratio,
    avg_disbursement_per_loan
from semantic.executive_monthly_trend
order by month_start_date;

-- 2) Branch ranking for a selected month
-- Replace :report_month with a value like '2024-06-01'.
select
    branch_id,
    total_loan_disbursement_amount,
    total_repayment_amount,
    total_claim_amount,
    total_cross_sell_customers,
    repayment_coverage_ratio,
    claim_to_disbursement_ratio,
    avg_disbursement_per_loan
from semantic.branch_monthly_kpi_base
where month_start_date = :report_month
order by total_loan_disbursement_amount desc;

-- 3) Branch risk watchlist (highest claim ratio)
select
    month_start_date,
    branch_id,
    total_claim_amount,
    total_loan_disbursement_amount,
    claim_to_disbursement_ratio
from semantic.branch_risk_watchlist
order by claim_to_disbursement_ratio desc, total_claim_amount desc
limit 30;
