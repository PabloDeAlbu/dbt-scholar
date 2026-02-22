with source as (
    select 
        * 
    from {{ source('dspace', 'collection2item') }}
),
renamed as (
    select 
        collection_id as collection_uuid,
        item_id as item_uuid,
        {{ dbt_date.today() }} as dv_load_datetime
    from source
),
ghost_record as (
    select
        -1 as collection_uuid,
        -1 as item_uuid,
        {{ dbt_date.today() }} as dv_load_datetime
)

select * from renamed
union all
select * from ghost_record
