with source as (
    select 
        * 
    from {{ source('dspace', 'community2community') }}
),
renamed as (
    select 
        parent_comm_id as parent_comm_uuid,
        child_comm_id as child_comm_uuid,
        {{ dbt_date.today() }} as _load_datetime
    from source
),
ghost_record as (
    select
        -1 as parent_comm_uuid,
        -1 as child_comm_uuid,
        {{ dbt_date.today() }} as _load_datetime
)

select * from renamed
union all
select * from ghost_record
