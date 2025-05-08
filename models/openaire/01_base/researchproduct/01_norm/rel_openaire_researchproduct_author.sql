with source as (
  select * from {{ source('openaire', 'rel_researchproduct_authors') }}
),
renamed as (
  select
    {{ adapter.quote("id") }} as researchproduct_id,
    {{ adapter.quote("fullName") }} as full_name,
    {{ adapter.quote("name") }} as name,
    {{ adapter.quote("surname") }} as surname,
    {{ adapter.quote("rank") }} as rank,
--    COALESCE({{ adapter.quote("pid") }}, 'NO DATA') as pid,
    {{ adapter.quote("pid.id.scheme") }} as pid_scheme,
    {{ adapter.quote("pid.id.value") }} as orcid,
    {{ adapter.quote("pid.provenance") }} as pid_provenance,
    load_datetime
  from source
),

casted as (
  select
    researchproduct_id::varchar,
    full_name::varchar,
    name::varchar,
    surname::varchar,
    rank::int,
    pid_scheme::varchar,
    orcid::varchar,
    pid_provenance::varchar,
    load_datetime::timestamp as load_datetime
  from renamed
),

fillna as (
  select
    COALESCE(researchproduct_id, 'NO DATA') as researchproduct_id,
    COALESCE(full_name, 'NO DATA') as full_name,
    COALESCE(name, 'NO DATA') as name,
    COALESCE(surname, 'NO DATA') as surname,
    COALESCE(rank, 0) as rank,
    COALESCE(pid_scheme, 'NO DATA') as pid_scheme,
    COALESCE(orcid, 'NO DATA') as orcid,
    COALESCE(pid_provenance, 'NO DATA') as pid_provenance,
    load_datetime
  from casted
)

select * from fillna
