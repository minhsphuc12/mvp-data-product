{% test unique_combination_of_columns(model, columns) %}

with grouped as (
    select
        {% for column in columns -%}
        {{ column }}{% if not loop.last %}, {% endif %}
        {%- endfor %},
        count(*) as row_count
    from {{ model }}
    group by
        {% for column in columns -%}
        {{ column }}{% if not loop.last %}, {% endif %}
        {%- endfor %}
)
select *
from grouped
where row_count > 1

{% endtest %}
