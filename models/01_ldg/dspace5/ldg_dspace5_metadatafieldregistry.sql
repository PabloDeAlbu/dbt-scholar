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
    {{ dbt_date.today() }} as _load_datetime
  from source
),
ghost_record as (
  select
    -1 as metadata_field_id,
    -1 as metadata_schema_id,
    '!UNKNOWN' as element,
    '!UNKNOWN' as qualifier,
    '!UNKNOWN' as scope_note,
    {{ dbt_date.today() }} as _load_datetime
)

select * from renamed
union all
select * from ghost_record
