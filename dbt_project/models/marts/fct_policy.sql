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
left join {{ ref('int_customer_360') }} c360
    on p.policy_holder_id = c360.insurance_policy_holder_id
left join {{ ref('dim_customer') }} dc
    on dc.customer_key = c360.master_customer_id
inner join {{ ref('dim_date') }} ds
    on ds.date_day = p.coverage_start_date
inner join {{ ref('dim_date') }} de
    on de.date_day = p.coverage_end_date
