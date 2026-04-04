{{ config(tags=["staging", "insurance"]) }}

select
    claim_id,
    policy_id,
    claim_amount,
    status as claim_status,
    filed_at,
    settled_at,
    'raw_insurance.claims' as record_source,
    'insurance_core' as source_system,
    coalesce(loaded_at, current_timestamp) as loaded_at
from {{ source('raw_insurance', 'claims') }}
