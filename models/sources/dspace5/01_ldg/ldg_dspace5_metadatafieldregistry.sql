with source as (
  select * from {{ source('dspace5', 'metadatafieldregistry') }}
),

renamed as (
  select
    metadata_field_id,
    metadata_schema_id,
    element,
    qualifier,
    scope_note,
    {{ dbt_date.today() }} as load_datetime
  from source
)

select * from renamed
