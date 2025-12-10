with source as (
  select * from {{ source('openaire', 'researchproduct_organizations') }}
),
renamed as (
  select
    researchproduct_id::text,
    organization_id::text,
    load_datetime::timestamp
  from source
)

select * from renamed
