{{ config(tags=["intermediate", "identity"]) }}
/*
  Deterministic identity resolution:
  1) national_id match (both sides non-null, equal)
  2) remaining: phone_number + normalized_full_name exact match
  3) otherwise unmatched (lending-only or insurance-only rows)
*/

with lending as (
    select * from {{ ref('stg_lending_customers') }}
),

insurance as (
    select * from {{ ref('stg_insurance_policy_holders') }}
),

nid_ranked as (
    select
        l.lending_customer_id,
        i.policy_holder_id,
        l.national_id,
        row_number() over (
            partition by l.national_id
            order by l.lending_customer_id, i.policy_holder_id
        ) as rn_l,
        row_number() over (
            partition by i.national_id
            order by i.policy_holder_id, l.lending_customer_id
        ) as rn_i
    from lending l
    inner join insurance i
        on l.national_id is not null
        and i.national_id is not null
        and l.national_id = i.national_id
),

nid_matches as (
    select
        lending_customer_id,
        policy_holder_id,
        national_id,
        'national_id' as match_method
    from nid_ranked
    where rn_l = 1
      and rn_i = 1
),

rem_lending as (
    select l.*
    from lending l
    where not exists (
        select 1
        from nid_matches n
        where n.lending_customer_id = l.lending_customer_id
    )
),

rem_insurance as (
    select i.*
    from insurance i
    where not exists (
        select 1
        from nid_matches n
        where n.policy_holder_id = i.policy_holder_id
    )
),

phone_ranked as (
    select
        l.lending_customer_id,
        i.policy_holder_id,
        row_number() over (
            partition by l.lending_customer_id
            order by i.policy_holder_id
        ) as rn_l,
        row_number() over (
            partition by i.policy_holder_id
            order by l.lending_customer_id
        ) as rn_i
    from rem_lending l
    inner join rem_insurance i
        on l.phone_number = i.phone_number
        and l.normalized_full_name = i.normalized_full_name
),

phone_matches as (
    select
        lending_customer_id,
        policy_holder_id,
        cast(null as varchar) as national_id,
        'phone_and_name' as match_method
    from phone_ranked
    where rn_l = 1
      and rn_i = 1
),

pairs as (
    select
        lending_customer_id,
        policy_holder_id,
        national_id,
        match_method
    from nid_matches
    union all
    select
        lending_customer_id,
        policy_holder_id,
        national_id,
        match_method
    from phone_matches
),

unmatched_lending as (
    select
        l.lending_customer_id,
        cast(null as integer) as policy_holder_id,
        l.national_id,
        'unmatched_lending' as match_method
    from lending l
    where not exists (
        select 1
        from pairs p
        where p.lending_customer_id = l.lending_customer_id
    )
),

unmatched_insurance as (
    select
        cast(null as integer) as lending_customer_id,
        i.policy_holder_id,
        i.national_id,
        'unmatched_insurance' as match_method
    from insurance i
    where not exists (
        select 1
        from pairs p
        where p.policy_holder_id = i.policy_holder_id
    )
),

unioned as (
    select * from pairs
    union all
    select * from unmatched_lending
    union all
    select * from unmatched_insurance
),

enriched as (
    select
        u.lending_customer_id,
        u.policy_holder_id,
        u.match_method,
        coalesce(l.national_id, i.national_id) as national_id,
        coalesce(l.phone_number, i.phone_number) as phone_number,
        coalesce(l.normalized_full_name, i.normalized_full_name) as normalized_full_name
    from unioned u
    left join lending l
        on u.lending_customer_id = l.lending_customer_id
    left join insurance i
        on u.policy_holder_id = i.policy_holder_id
)

select
    case
        when match_method = 'national_id' then md5('nid|' || coalesce(national_id, ''))
        when match_method = 'phone_and_name' then md5(
            'ph|' || coalesce(phone_number, '') || '|' || coalesce(normalized_full_name, '')
        )
        when match_method = 'unmatched_lending' then md5('len|' || lending_customer_id::text)
        else md5('ins|' || policy_holder_id::text)
    end as master_customer_id,
    lending_customer_id,
    policy_holder_id as insurance_policy_holder_id,
    match_method,
    national_id,
    phone_number,
    normalized_full_name
from enriched
