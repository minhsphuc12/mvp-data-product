{{ config(tags=["dimension"]) }}

with snapshots as (
    select
        branch_id,
        branch_name,
        city,
        opened_at,
        record_source,
        source_system,
        loaded_at as valid_from_ts,
        md5(
            coalesce(branch_name, '') || '|' ||
            coalesce(city, '') || '|' ||
            coalesce(opened_at::text, '')
        ) as tracked_hash
    from {{ ref('stg_lending_branches') }}
),
changes as (
    select
        *,
        lag(tracked_hash) over (
            partition by branch_id
            order by valid_from_ts
        ) as prev_tracked_hash
    from snapshots
),
version_starts as (
    select *
    from changes
    where prev_tracked_hash is null
       or prev_tracked_hash <> tracked_hash
),
versioned as (
    select
        *,
        row_number() over (
            partition by branch_id
            order by valid_from_ts
        ) as version_number,
        lead(valid_from_ts) over (
            partition by branch_id
            order by valid_from_ts
        ) as valid_to_ts
    from version_starts
)
select
    {{ surrogate_key(['branch_id', 'valid_from_ts']) }} as branch_key,
    branch_id,
    branch_name,
    city,
    opened_at,
    record_source,
    source_system,
    valid_from_ts,
    valid_to_ts,
    (valid_to_ts is null) as is_current,
    version_number,
    current_timestamp as loaded_at
from versioned
