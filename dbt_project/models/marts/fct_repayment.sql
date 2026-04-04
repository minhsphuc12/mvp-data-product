{{ config(tags=["fact"]) }}

select
    r.repayment_id,
    dd.date_key as payment_date_key,
    db.branch_key,
    dc.customer_key,
    l.loan_id,
    l.branch_id,
    r.paid_at,
    r.amount as repayment_amount,
    r.repayment_status,
    r.record_source,
    r.source_system,
    current_timestamp as loaded_at
from {{ ref('stg_lending_repayments') }} r
inner join {{ ref('stg_lending_loans') }} l
    on r.loan_id = l.loan_id
inner join {{ ref('int_customer_360') }} c360
    on l.lending_customer_id = c360.lending_customer_id
inner join {{ ref('dim_date') }} dd
    on dd.date_day = (r.paid_at::date)
inner join {{ ref('dim_branch') }} db
    on db.branch_id = l.branch_id
inner join {{ ref('dim_customer') }} dc
    on dc.customer_key = c360.master_customer_id
