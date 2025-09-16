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
)

SELECT * FROM casted