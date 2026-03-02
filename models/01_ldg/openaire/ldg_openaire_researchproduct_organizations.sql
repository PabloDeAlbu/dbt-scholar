with source as (
  select * from {{ source('openaire', 'researchproduct_organizations') }}
),
renamed as (
  select
    researchproduct_id::text,
    organization_id::text,
    _load_datetime::timestamp
  from source
),
ghost_record as (
  select
    '!UNKNOWN'::text as researchproduct_id,
    '!UNKNOWN'::text as organization_id,
    {{ dbt_date.today() }} as _load_datetime
)

select * from renamed
union all
select * from ghost_record
