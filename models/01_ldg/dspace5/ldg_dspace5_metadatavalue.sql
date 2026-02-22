with source as (
  select * from {{ source('dspace5', 'metadatavalue') }}
),
renamed as (
  select
    metadata_value_id,
    resource_id,
    metadata_field_id,
    text_value,
    text_lang,
    place,
    authority,
    confidence,
    resource_type_id,
    {{ dbt_date.today() }} as dv_load_datetime
  from source
),
ghost_record as (
  select
    -1 as metadata_value_id,
    -1 as resource_id,
    -1 as metadata_field_id,
    '!UNKNOWN' as text_value,
    '!UNKNOWN' as text_lang,
    -1 as place,
    '!UNKNOWN' as authority,
    -1 as confidence,
    -1 as resource_type_id,
    {{ dbt_date.today() }} as dv_load_datetime
)

select * from renamed
union all
select * from ghost_record
