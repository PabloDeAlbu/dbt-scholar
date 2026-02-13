with source as (
    select 
        * 
    from {{ source('dspace', 'community2collection') }}
),
renamed as (
    select 
        community_id as community_uuid,
        collection_id as collection_uuid,
        {{ dbt_date.today() }} as load_datetime
    from source
),
ghost_record as (
    select
        -1 as community_uuid,
        -1 as collection_uuid,
        {{ dbt_date.today() }} as load_datetime
)

select * from renamed
union all
select * from ghost_record
