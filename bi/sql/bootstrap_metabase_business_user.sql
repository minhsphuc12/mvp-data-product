-- Bootstrap a real login role for Metabase business users.
-- Run as a privileged database role on analytics_db.
--
-- Usage (psql example):
--   psql "$ANALYTICS_DATABASE_URL" \
--     -v metabase_bi_user='metabase_business' \
--     -v metabase_bi_password='change_me_strong_password' \
--     -f bi/sql/bootstrap_metabase_business_user.sql

\if :{?metabase_bi_user}
\else
\echo 'ERROR: missing psql variable metabase_bi_user'
\quit 1
\endif

\if :{?metabase_bi_password}
\else
\echo 'ERROR: missing psql variable metabase_bi_password'
\quit 1
\endif

select format(
    'create role %I login password %L',
    :'metabase_bi_user',
    :'metabase_bi_password'
)
where not exists (select 1 from pg_roles where rolname = :'metabase_bi_user')
\gexec

select format(
    'alter role %I login password %L',
    :'metabase_bi_user',
    :'metabase_bi_password'
)
where exists (select 1 from pg_roles where rolname = :'metabase_bi_user')
\gexec

select format(
    'grant bi_business_readonly to %I',
    :'metabase_bi_user'
)
\gexec

select format(
    'alter role %I in database %I set search_path = semantic, marts, public',
    :'metabase_bi_user',
    current_database()
)
\gexec
