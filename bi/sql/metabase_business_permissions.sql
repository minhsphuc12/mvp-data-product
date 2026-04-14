-- Read-only role for BI tools (Metabase) on marts + semantic.
-- Run as the warehouse owner role (same as ANALYTICS_DB_USER) so default
-- privileges match objects created by dbt / semantic builders.

-- 1) Create read-only role for business BI users.
do $$
begin
    if not exists (select 1 from pg_roles where rolname = 'bi_business_readonly') then
        create role bi_business_readonly;
    end if;
end $$;

-- 2) Ensure role can connect and use marts + semantic schemas.
do $grant$
begin
    execute format('grant connect on database %I to bi_business_readonly', current_database());
end
$grant$;

create schema if not exists semantic;

grant usage on schema marts to bi_business_readonly;
grant usage on schema semantic to bi_business_readonly;

-- 3) Grant read on all existing tables and views in marts and semantic.
grant select on all tables in schema marts to bi_business_readonly;
grant select on all tables in schema semantic to bi_business_readonly;

-- 4) Keep future objects readable (objects created by the session role).
alter default privileges in schema marts
grant select on tables to bi_business_readonly;

alter default privileges in schema semantic
grant select on tables to bi_business_readonly;
