{{ config(tags=["staging", "lending"]) }}

select
    repayment_id,
    loan_id,
    amount,
    paid_at,
    status as repayment_status,
    'raw_lending.repayments' as record_source,
    'lending_core' as source_system,
    coalesce(loaded_at, current_timestamp) as loaded_at
from {{ source('raw_lending', 'repayments') }}
