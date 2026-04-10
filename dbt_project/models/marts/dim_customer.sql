{{ config(tags=["dimension"]) }}

with snapshots as (
    select
        master_customer_id,
        lending_customer_id,
        insurance_policy_holder_id,
        match_method,
        national_id,
        phone_number,
        normalized_full_name,
        primary_branch_id,
        lending_full_name,
        insurance_full_name,
        lending_email,
        insurance_email,
        record_source,
        loaded_at as valid_from_ts,
        md5(
            coalesce(match_method, '') || '|' ||
            coalesce(national_id, '') || '|' ||
            coalesce(phone_number, '') || '|' ||
            coalesce(normalized_full_name, '') || '|' ||
            coalesce(primary_branch_id::text, '') || '|' ||
            coalesce(lending_full_name, '') || '|' ||
            coalesce(insurance_full_name, '') || '|' ||
            coalesce(lending_email, '') || '|' ||
            coalesce(insurance_email, '')
        ) as tracked_hash
    from {{ ref('int_customer_360') }}
),
changes as (
    select
        *,
        lag(tracked_hash) over (
            partition by master_customer_id
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
            partition by master_customer_id
            order by valid_from_ts
        ) as version_number,
        lead(valid_from_ts) over (
            partition by master_customer_id
            order by valid_from_ts
        ) as valid_to_ts
    from version_starts
)
select
    {{ surrogate_key(['master_customer_id', 'valid_from_ts']) }} as customer_key,
    master_customer_id,
    lending_customer_id,
    insurance_policy_holder_id,
    match_method,
    national_id,
    phone_number,
    normalized_full_name,
    primary_branch_id,
    lending_full_name,
    insurance_full_name,
    lending_email,
    insurance_email,
    record_source,
    valid_from_ts,
    valid_to_ts,
    (valid_to_ts is null) as is_current,
    version_number,
    current_timestamp as loaded_at
from versioned
