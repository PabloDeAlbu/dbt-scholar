with source as (
  select * from {{ source('openaire', 'rel_researchproduct_authors') }}
),
renamed as (
  select
    id as researchproduct_id,
    {{ adapter.quote("fullName") }} as full_name,
    name,
    surname,
    rank,
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
    load_datetime::timestamp
  from renamed
)

select * from casted where pid_scheme is not null
