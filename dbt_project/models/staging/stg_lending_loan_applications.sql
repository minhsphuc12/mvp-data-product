{{ config(tags=["staging", "lending"]) }}

select
    application_id,
    customer_id as lending_customer_id,
    branch_id,
    amount_requested,
    status as application_status,
    applied_at,
    'staging.lending_loan_applications' as record_source,
    'lending_core' as source_system,
    coalesce(loaded_at, current_timestamp) as loaded_at
from {{ source('staging_lending', 'lending_loan_applications') }}
