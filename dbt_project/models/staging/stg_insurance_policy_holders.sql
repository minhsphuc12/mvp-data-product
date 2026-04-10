{{ config(tags=["staging", "insurance"]) }}

select
    policy_holder_id,
    national_id,
    phone_number,
    full_name,
    trim(regexp_replace(upper(full_name), '[[:space:]]+', ' ', 'g')) as normalized_full_name,
    email,
    created_at,
    'staging.insurance_policy_holders' as record_source,
    'insurance_core' as source_system,
    coalesce(loaded_at, current_timestamp) as loaded_at
from {{ source('staging_insurance', 'insurance_policy_holders') }}
