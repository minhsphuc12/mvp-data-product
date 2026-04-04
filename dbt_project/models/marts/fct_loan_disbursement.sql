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
inner join {{ ref('int_customer_360') }} c360
    on l.lending_customer_id = c360.lending_customer_id
inner join {{ ref('dim_date') }} dd
    on dd.date_day = l.disbursement_date
inner join {{ ref('dim_branch') }} db
    on db.branch_id = l.branch_id
inner join {{ ref('dim_customer') }} dc
    on dc.customer_key = c360.master_customer_id
