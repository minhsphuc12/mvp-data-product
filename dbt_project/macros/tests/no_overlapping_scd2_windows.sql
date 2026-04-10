{% test no_overlapping_scd2_windows(model, business_key, valid_from, valid_to) %}

with ordered as (
    select
        {{ business_key }} as business_key,
        {{ valid_from }} as valid_from_ts,
        {{ valid_to }} as valid_to_ts,
        lead({{ valid_from }}) over (
            partition by {{ business_key }}
            order by {{ valid_from }}
        ) as next_valid_from_ts
    from {{ model }}
),
violations as (
    select *
    from ordered
    where valid_to_ts is not null
      and next_valid_from_ts is not null
      and valid_to_ts > next_valid_from_ts
)
select *
from violations

{% endtest %}
