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
left join lateral (
    select d.customer_key
    from {{ ref('dim_customer') }} d
    where d.insurance_policy_holder_id = p.policy_holder_id
      and d.valid_from_ts <= c.filed_at
    order by d.valid_from_ts desc
    limit 1
) dc on true
inner join {{ ref('dim_date') }} dd
    on dd.date_day = (c.filed_at::date)
