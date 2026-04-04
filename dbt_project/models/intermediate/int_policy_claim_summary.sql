{{ config(tags=["intermediate"]) }}

select
    p.policy_id,
    p.policy_holder_id,
    p.policy_number,
    p.product_type,
    p.premium_amount,
    p.coverage_start_date,
    p.coverage_end_date,
    p.policy_status,
    count(c.claim_id) as claim_count,
    sum(
        case when c.claim_status = 'approved' then c.claim_amount else 0 end
    ) as approved_claim_amount,
    sum(
        case when c.claim_status in ('approved', 'denied', 'closed') then c.claim_amount else 0 end
    ) as settled_claim_amount
from {{ ref('stg_insurance_policies') }} p
left join {{ ref('stg_insurance_claims') }} c
    on p.policy_id = c.policy_id
group by
    p.policy_id,
    p.policy_holder_id,
    p.policy_number,
    p.product_type,
    p.premium_amount,
    p.coverage_start_date,
    p.coverage_end_date,
    p.policy_status
