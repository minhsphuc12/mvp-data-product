{{ config(tags=["dimension"]) }}

select
    master_customer_id as customer_key,
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
    loaded_at
from {{ ref('int_customer_360') }}
