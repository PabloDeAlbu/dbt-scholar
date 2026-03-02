with source as (
    select 
        * 
    from {{ source('dspace', 'community') }}
),
renamed as (
    select 
        community_id,
        uuid as community_uuid,
        admin,
        logo_bitstream_id,
        {{ dbt_date.today() }} as _load_datetime
    from source
),
ghost_record as (
    select
        -1 as community_id,
        '!UNKNOWN' as community_uuid,
        false as admin,
        -1 as logo_bitstream_id,
        {{ dbt_date.today() }} as _load_datetime
)

select * from renamed
union all
select * from ghost_record
