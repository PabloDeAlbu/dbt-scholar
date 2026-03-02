with source as (
    select 
        * 
    from {{ source('dspace', 'community2collection') }}
),
renamed as (
    select 
        community_id as community_uuid,
        collection_id as collection_uuid,
        {{ dbt_date.today() }} as _load_datetime
    from source
),
ghost_record as (
    select
        '00000000-0000-0000-0000-000000000000'::uuid as community_uuid,
        '00000000-0000-0000-0000-000000000000'::uuid as collection_uuid,
        {{ dbt_date.today() }} as _load_datetime
)

select * from renamed
union all
select * from ghost_record
