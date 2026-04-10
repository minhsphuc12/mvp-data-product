{{ config(tags=["intermediate"]) }}

select
    r.master_customer_id,
    r.lending_customer_id,
    r.insurance_policy_holder_id,
    r.match_method,
    r.national_id,
    r.phone_number,
    r.normalized_full_name,
    l.primary_branch_id,
    l.full_name as lending_full_name,
    i.full_name as insurance_full_name,
    l.email as lending_email,
    i.email as insurance_email,
    'int_customer_identity_resolution' as record_source,
    r.loaded_at
from {{ ref('int_customer_identity_resolution') }} r
left join {{ ref('stg_lending_customers') }} l
    on r.lending_customer_id = l.lending_customer_id
    and r.loaded_at = l.loaded_at
left join {{ ref('stg_insurance_policy_holders') }} i
    on r.insurance_policy_holder_id = i.policy_holder_id
    and r.loaded_at = i.loaded_at
