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
        {{ dbt_date.today() }} as _load_datetime
    from source
),
ghost_record as (
    select
        -1 as collection_id,
        '00000000-0000-0000-0000-000000000000'::uuid as collection_uuid,
        '00000000-0000-0000-0000-000000000000'::uuid as submitter,
        '00000000-0000-0000-0000-000000000000'::uuid as template_item_id,
        '00000000-0000-0000-0000-000000000000'::uuid as logo_bitstream_id,
        '00000000-0000-0000-0000-000000000000'::uuid as admin,
        {{ dbt_date.today() }} as _load_datetime
)

select * from renamed
union all
select * from ghost_record
