with source as (
    select 
        * 
    from {{ source('dspace', 'item') }}
),
renamed as (
    select
        item_id,
        uuid as item_uuid,
        in_archive,
        withdrawn,
        last_modified,
        discoverable,
        submitter_id,
        owning_collection,
        {{ dbt_date.today() }} as load_datetime
    from source
),
ghost_record as (
    select
        -1 as item_id,
        '!UNKNOWN' as item_uuid,
        false as in_archive,
        false as withdrawn,
        '1900-01-01'::timestamp as last_modified,
        false as discoverable,
        -1 as submitter_id,
        -1 as owning_collection,
        {{ dbt_date.today() }} as load_datetime
)

select * from renamed
union all
select * from ghost_record
