-- Restrict business users to semantic curated artifacts only.
-- Run as a privileged role on analytics_db.

-- 1) Create read-only role for business BI users.
do $$
begin
    if not exists (select 1 from pg_roles where rolname = 'bi_business_readonly') then
        create role bi_business_readonly;
    end if;
end $$;

-- 2) Ensure role can connect and use semantic schema.
grant connect on database analytics_db to bi_business_readonly;
grant usage on schema semantic to bi_business_readonly;

-- 3) Grant read on all existing semantic views/tables.
grant select on all tables in schema semantic to bi_business_readonly;

-- 4) Keep future semantic objects readable.
alter default privileges in schema semantic
grant select on tables to bi_business_readonly;

-- 5) Optional hardening: explicitly deny marts schema.
revoke all privileges on schema marts from bi_business_readonly;
