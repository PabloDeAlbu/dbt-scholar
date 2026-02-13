with source as (
  select * from {{ source('openaire', 'researchproduct_collectedfrom') }}
),
renamed as (
  select
    researchproduct_id::text,
    datasource_id::text,
    value::text,
    load_datetime::timestamp
  from source
),
ghost_record as (
  select
    '!UNKNOWN'::text as researchproduct_id,
    '!UNKNOWN'::text as datasource_id,
    '!UNKNOWN'::text as value,
    {{ dbt_date.today() }} as load_datetime
)

select * from renamed
union all
select * from ghost_record
