{{ config(tags=["fact"]) }}

select
    c.claim_id,
    dd.date_key as filed_date_key,
    p.policy_id,
    dc.customer_key,
    c.claim_amount,
    c.claim_status,
    c.filed_at,
    c.settled_at,
    c.record_source,
    c.source_system,
    current_timestamp as loaded_at
from {{ ref('stg_insurance_claims') }} c
inner join {{ ref('stg_insurance_policies') }} p
    on c.policy_id = p.policy_id
left join {{ ref('int_customer_360') }} c360
    on p.policy_holder_id = c360.insurance_policy_holder_id
left join {{ ref('dim_customer') }} dc
    on dc.customer_key = c360.master_customer_id
inner join {{ ref('dim_date') }} dd
    on dd.date_day = (c.filed_at::date)
