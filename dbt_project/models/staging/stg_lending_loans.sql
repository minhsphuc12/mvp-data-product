{{
  config(
    materialized='incremental',
    unique_key='loan_id',
    incremental_strategy='merge',
    tags=['staging', 'lending'],
  )
}}
/*
  Incremental staging example: refresh rows from staging landing when loaded_at advances (CDC / micro-batch).
  Full refresh: dbt run --select stg_lending_loans --full-refresh
*/

select
    loan_id,
    application_id,
    customer_id as lending_customer_id,
    branch_id,
    principal_amount,
    status as loan_status,
    disbursement_date,
    created_at,
    'staging.lending_loans' as record_source,
    'lending_core' as source_system,
    coalesce(loaded_at, current_timestamp) as loaded_at
from {{ source('staging_lending', 'lending_loans') }}
{% if is_incremental() %}
where coalesce(loaded_at, current_timestamp) > (select coalesce(max(loaded_at), '1970-01-01'::timestamptz) from {{ this }})
{% endif %}
