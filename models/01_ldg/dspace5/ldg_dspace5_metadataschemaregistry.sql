with source as (
  select * from {{ source('dspace5', 'metadataschemaregistry') }}
),

renamed as (
  select
    metadata_schema_id,
    namespace,
    short_id,
    {{ dbt_date.today() }} as load_datetime
  from source
)

select * from renamed
