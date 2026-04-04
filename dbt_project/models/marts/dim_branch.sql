{{ config(tags=["dimension"]) }}

select
    {{ surrogate_key(['branch_id']) }} as branch_key,
    branch_id,
    branch_name,
    city,
    opened_at,
    record_source,
    source_system,
    loaded_at
from {{ ref('stg_lending_branches') }}
