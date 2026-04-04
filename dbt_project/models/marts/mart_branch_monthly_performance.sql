{{ config(tags=["mart", "aggregate"]) }}
/*
  Grain: branch_id × calendar month (month_start_date).
  active_policy_count / claims / cross_sell: only customers with a lending primary_branch_id
  (insurance-only policy holders are excluded from branch attribution).
*/

with disburse as (
    select
        branch_id,
        date_trunc('month', disbursement_date)::date as month_start_date,
        sum(loan_disbursement_amount) as loan_disbursement_amount,
        count(*) as number_of_loans
    from {{ ref('fct_loan_disbursement') }}
    group by branch_id, date_trunc('month', disbursement_date)::date
),

repay as (
    select
        l.branch_id,
        date_trunc('month', r.paid_at)::date as month_start_date,
        sum(
            case when r.repayment_status = 'completed' then r.amount else 0 end
        ) as repayment_amount
    from {{ ref('stg_lending_repayments') }} r
    inner join {{ ref('stg_lending_loans') }} l
        on r.loan_id = l.loan_id
    group by l.branch_id, date_trunc('month', r.paid_at)::date
),

policy_active as (
    select
        c.primary_branch_id as branch_id,
        date_trunc('month', dd.date_day)::date as month_start_date,
        count(distinct p.policy_id) as active_policy_count
    from {{ ref('stg_insurance_policies') }} p
    inner join {{ ref('int_customer_360') }} c
        on p.policy_holder_id = c.insurance_policy_holder_id
    inner join {{ ref('dim_date') }} dd
        on dd.date_day >= p.coverage_start_date
        and dd.date_day <= p.coverage_end_date
    where p.policy_status = 'active'
      and c.primary_branch_id is not null
    group by c.primary_branch_id, date_trunc('month', dd.date_day)::date
),

claims as (
    select
        c.primary_branch_id as branch_id,
        date_trunc('month', cl.filed_at)::date as month_start_date,
        sum(cl.claim_amount) as claim_amount
    from {{ ref('stg_insurance_claims') }} cl
    inner join {{ ref('stg_insurance_policies') }} p
        on cl.policy_id = p.policy_id
    inner join {{ ref('int_customer_360') }} c
        on p.policy_holder_id = c.insurance_policy_holder_id
    where c.primary_branch_id is not null
    group by c.primary_branch_id, date_trunc('month', cl.filed_at)::date
),

lending_customers_month as (
    select distinct
        l.branch_id,
        date_trunc('month', l.disbursement_date)::date as month_start_date,
        c.master_customer_id
    from {{ ref('stg_lending_loans') }} l
    inner join {{ ref('int_customer_360') }} c
        on l.lending_customer_id = c.lending_customer_id
),

policy_customers_month as (
    select distinct
        c.primary_branch_id as branch_id,
        date_trunc('month', dd.date_day)::date as month_start_date,
        c.master_customer_id
    from {{ ref('stg_insurance_policies') }} p
    inner join {{ ref('int_customer_360') }} c
        on p.policy_holder_id = c.insurance_policy_holder_id
    inner join {{ ref('dim_date') }} dd
        on dd.date_day >= p.coverage_start_date
        and dd.date_day <= p.coverage_end_date
    where p.policy_status = 'active'
      and c.primary_branch_id is not null
),

cross_sell as (
    select
        l.branch_id,
        l.month_start_date,
        count(distinct l.master_customer_id) as cross_sell_customer_count
    from lending_customers_month l
    inner join policy_customers_month p
        on l.master_customer_id = p.master_customer_id
        and l.branch_id = p.branch_id
        and l.month_start_date = p.month_start_date
    group by l.branch_id, l.month_start_date
),

branch_keys as (
    select branch_id, month_start_date from disburse
    union
    select branch_id, month_start_date from repay
    union
    select branch_id, month_start_date from policy_active
    union
    select branch_id, month_start_date from claims
    union
    select branch_id, month_start_date from cross_sell
)

select
    md5(
        k.branch_id::text || '|' || to_char(k.month_start_date, 'YYYY-MM-DD')
    ) as branch_month_sk,
    k.branch_id,
    k.month_start_date,
    coalesce(d.loan_disbursement_amount, 0) as loan_disbursement_amount,
    coalesce(d.number_of_loans, 0) as number_of_loans,
    coalesce(r.repayment_amount, 0) as repayment_amount,
    coalesce(pa.active_policy_count, 0) as active_policy_count,
    coalesce(cl.claim_amount, 0) as claim_amount,
    coalesce(cs.cross_sell_customer_count, 0) as cross_sell_customer_count,
    current_timestamp as loaded_at
from branch_keys k
left join disburse d
    on k.branch_id = d.branch_id
    and k.month_start_date = d.month_start_date
left join repay r
    on k.branch_id = r.branch_id
    and k.month_start_date = r.month_start_date
left join policy_active pa
    on k.branch_id = pa.branch_id
    and k.month_start_date = pa.month_start_date
left join claims cl
    on k.branch_id = cl.branch_id
    and k.month_start_date = cl.month_start_date
left join cross_sell cs
    on k.branch_id = cs.branch_id
    and k.month_start_date = cs.month_start_date
