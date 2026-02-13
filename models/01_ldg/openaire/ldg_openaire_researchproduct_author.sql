with source as (
  select * from {{ source('openaire', 'researchproduct_authors') }}
),
casted as (
  select
    id::text as researchproduct_id,
    name::text,
    rank::int,
    surname::text,
    {{ adapter.quote("fullName") }}::text as full_name,
    {{ adapter.quote("pid.id.scheme") }}::text as pid_scheme,
    {{ adapter.quote("pid.id.value") }}::text as orcid,
    {{ adapter.quote("pid.provenance") }}::text as pid_provenance,
    load_datetime::timestamp
  from source
),
ghost_record as (
  select
    '!UNKNOWN'::text as researchproduct_id,
    '!UNKNOWN'::text as name,
    -1::int as rank,
    '!UNKNOWN'::text as surname,
    '!UNKNOWN'::text as full_name,
    '!UNKNOWN'::text as pid_scheme,
    '!UNKNOWN'::text as orcid,
    '!UNKNOWN'::text as pid_provenance,
    {{ dbt_date.today() }} as load_datetime
)

SELECT * FROM casted
union all
select * from ghost_record
