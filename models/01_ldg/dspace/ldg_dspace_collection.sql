with source as (
    select 
        * 
    from {{ source('dspace', 'collection') }}
),
renamed as (
    select 
        collection_id,
        uuid as collection_uuid,
        submitter,
        template_item_id,
        logo_bitstream_id,
        admin,
        {{ dbt_date.today() }} as load_datetime
    from source
),
ghost_record as (
    select
        -1 as collection_id,
        '!UNKNOWN' as collection_uuid,
        false as submitter,
        -1 as template_item_id,
        -1 as logo_bitstream_id,
        false as admin,
        {{ dbt_date.today() }} as load_datetime
)

select * from renamed
union all
select * from ghost_record
