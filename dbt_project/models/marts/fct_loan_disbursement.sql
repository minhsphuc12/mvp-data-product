{{ config(tags=["fact"]) }}

select
    l.loan_id,
    dd.date_key as disbursement_date_key,
    db.branch_key,
    dc.customer_key,
    l.branch_id,
    l.disbursement_date,
    l.principal_amount as loan_disbursement_amount,
    l.loan_status,
    l.record_source,
    l.source_system,
    current_timestamp as loaded_at
from {{ ref('stg_lending_loans') }} l
inner join {{ ref('dim_date') }} dd
    on dd.date_day = l.disbursement_date
inner join lateral (
    select d.branch_key
    from {{ ref('dim_branch') }} d
    where d.branch_id = l.branch_id
      and d.valid_from_ts <= l.disbursement_date::timestamp
    order by d.valid_from_ts desc
    limit 1
) db on true
inner join lateral (
    select d.customer_key
    from {{ ref('dim_customer') }} d
    where d.lending_customer_id = l.lending_customer_id
      and d.valid_from_ts <= l.disbursement_date::timestamp
    order by d.valid_from_ts desc
    limit 1
) dc on true
