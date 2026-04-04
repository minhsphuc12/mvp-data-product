{{ config(tags=["staging", "lending"]) }}

select
    loan_id,
    application_id,
    customer_id as lending_customer_id,
    branch_id,
    principal_amount,
    status as loan_status,
    disbursement_date,
    created_at,
    'raw_lending.loans' as record_source,
    'lending_core' as source_system,
    coalesce(loaded_at, current_timestamp) as loaded_at
from {{ source('raw_lending', 'loans') }}
