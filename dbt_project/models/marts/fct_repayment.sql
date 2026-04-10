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
inner join {{ ref('dim_date') }} dd
    on dd.date_day = (r.paid_at::date)
inner join lateral (
    select d.branch_key
    from {{ ref('dim_branch') }} d
    where d.branch_id = l.branch_id
      and d.valid_from_ts <= r.paid_at
    order by d.valid_from_ts desc
    limit 1
) db on true
inner join lateral (
    select d.customer_key
    from {{ ref('dim_customer') }} d
    where d.lending_customer_id = l.lending_customer_id
      and d.valid_from_ts <= r.paid_at
    order by d.valid_from_ts desc
    limit 1
) dc on true
