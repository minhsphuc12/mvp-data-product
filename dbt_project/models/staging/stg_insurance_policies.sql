{{ config(tags=["staging", "insurance"]) }}

select
    policy_id,
    policy_holder_id,
    policy_number,
    product_type,
    premium_amount,
    coverage_start_date,
    coverage_end_date,
    status as policy_status,
    'staging.insurance_policies' as record_source,
    'insurance_core' as source_system,
    coalesce(loaded_at, current_timestamp) as loaded_at
from {{ source('staging_insurance', 'insurance_policies') }}
