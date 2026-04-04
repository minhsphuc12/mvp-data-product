{{ config(tags=["staging", "lending"]) }}

select
    customer_id as lending_customer_id,
    national_id,
    phone_number,
    full_name,
    trim(regexp_replace(upper(full_name), '[[:space:]]+', ' ', 'g')) as normalized_full_name,
    email,
    primary_branch_id,
    created_at,
    'raw_lending.customers' as record_source,
    'lending_core' as source_system,
    coalesce(loaded_at, current_timestamp) as loaded_at
from {{ source('raw_lending', 'customers') }}
