with source as (
  select * from {{ source('openaire', 'researchproduct_authors') }}
),
casted as (
  select
    id::text as researchproduct_id,
    {{ adapter.quote("fullName") }}::text as full_name,
    name::text,
--    pid,
    rank,
    surname,
    {{ adapter.quote("pid.id.scheme") }} as pid_scheme,
    {{ adapter.quote("pid.id.value") }} as orcid,
    {{ adapter.quote("pid.provenance") }} as pid_provenance,
    load_datetime::timestamp
  from source
)

SELECT * FROM casted