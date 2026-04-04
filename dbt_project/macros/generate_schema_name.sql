{# Use folder +schema as the exact Postgres schema name (no target_ prefix). #}
{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- if custom_schema_name is not none and custom_schema_name | trim != '' -%}
    {{ custom_schema_name | trim }}
  {%- else -%}
    {{ target.schema }}
  {%- endif -%}
{%- endmacro %}
