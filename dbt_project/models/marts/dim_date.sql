{{ config(tags=["dimension"]) }}

with days as (
    select generate_series(
        (current_date - interval '800 days')::date,
        (current_date + interval '800 days')::date,
        interval '1 day'
    )::date as date_day
)

select
    to_char(date_day, 'YYYYMMDD')::integer as date_key,
    date_day,
    extract(isodow from date_day)::integer as iso_day_of_week,
    trim(to_char(date_day, 'Month')) as month_name,
    extract(year from date_day)::integer as year_number,
    extract(month from date_day)::integer as month_number,
    to_char(date_day, 'YYYY-MM') as year_month_label,
    (date_trunc('month', date_day))::date as month_start_date,
    case when extract(isodow from date_day) in (6, 7) then true else false end as is_weekend
from days
