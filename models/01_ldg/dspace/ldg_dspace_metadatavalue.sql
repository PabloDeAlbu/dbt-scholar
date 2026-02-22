with source as (
    select 
        * 
    from {{ source('dspace', 'metadatavalue') }}
),
renamed as (
    select 
        metadata_value_id,
        metadata_field_id,
        text_value,
        text_lang,
        place,
        authority,
        confidence,
        dspace_object_id,
        {{ dbt_date.today() }} as dv_load_datetime
    from source
),
ghost_record as (
    select
        -1 as metadata_value_id,
        -1 as metadata_field_id,
        '!UNKNOWN' as text_value,
        '!UNKNOWN' as text_lang,
        -1 as place,
        '!UNKNOWN' as authority,
        -1 as confidence,
        '00000000-0000-0000-0000-000000000000'::uuid as dspace_object_id,
        {{ dbt_date.today() }} as dv_load_datetime
)

select * from renamed
union all
select * from ghost_record
