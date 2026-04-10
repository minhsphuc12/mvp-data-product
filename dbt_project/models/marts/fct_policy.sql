{{ config(tags=["fact"]) }}

select
    p.policy_id,
    ds.date_key as coverage_start_date_key,
    de.date_key as coverage_end_date_key,
    dc.customer_key,
    p.policy_holder_id,
    p.policy_number,
    p.product_type,
    p.premium_amount,
    p.coverage_start_date,
    p.coverage_end_date,
    p.policy_status,
    p.record_source,
    p.source_system,
    current_timestamp as loaded_at
from {{ ref('stg_insurance_policies') }} p
left join lateral (
    select d.customer_key
    from {{ ref('dim_customer') }} d
    where d.insurance_policy_holder_id = p.policy_holder_id
      and d.valid_from_ts <= p.coverage_start_date::timestamp
    order by d.valid_from_ts desc
    limit 1
) dc on true
inner join {{ ref('dim_date') }} ds
    on ds.date_day = p.coverage_start_date
inner join {{ ref('dim_date') }} de
    on de.date_day = p.coverage_end_date
