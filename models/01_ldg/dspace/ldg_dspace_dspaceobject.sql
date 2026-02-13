with source as (
    select 
        * 
    from {{ source('dspace', 'dspaceobject') }}
),
renamed as (
    select 
        uuid,
        {{ dbt_date.today() }} as load_datetime
    from source
),
ghost_record as (
    select
        '!UNKNOWN' as uuid,
        {{ dbt_date.today() }} as load_datetime
)

select * from renamed
union all
select * from ghost_record
