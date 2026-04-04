{{ config(tags=["staging", "lending"]) }}

select
    branch_id,
    branch_name,
    city,
    opened_at,
    'raw_lending.branches' as record_source,
    'lending_core' as source_system,
    coalesce(loaded_at, current_timestamp) as loaded_at
from {{ source('raw_lending', 'branches') }}
