{% macro surrogate_key(field_exprs) -%}
  md5(
    {% for f in field_exprs -%}
      coalesce(cast({{ f }} as text), ''){% if not loop.last %} || '|' || {% endif %}
    {%- endfor %}
  )
{%- endmacro %}
