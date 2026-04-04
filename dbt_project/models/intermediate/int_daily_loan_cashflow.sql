{{ config(tags=["intermediate"]) }}

select
    r.loan_id,
    l.lending_customer_id,
    l.branch_id,
    (r.paid_at::date) as cashflow_date,
    sum(case when r.repayment_status = 'completed' then r.amount else 0 end) as repayment_amount_completed,
    sum(r.amount) as repayment_amount_all,
    count(*) as repayment_event_count
from {{ ref('stg_lending_repayments') }} r
inner join {{ ref('stg_lending_loans') }} l
    on r.loan_id = l.loan_id
group by
    r.loan_id,
    l.lending_customer_id,
    l.branch_id,
    (r.paid_at::date)
