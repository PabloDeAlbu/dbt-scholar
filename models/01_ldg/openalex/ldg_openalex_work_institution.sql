with source as (
    select
        work_id,
        id as institution_id,
        country_code,
        display_name,
        ror,
        type,
        dv_load_datetime
    from {{ source('openalex', 'map_work_institution') }}
),
casted as (
    select
        work_id::text,
        institution_id::text,
        country_code::text,
        display_name::text,
        ror::text,
        type::text,
       dv_load_datetime::timestamp
    from source
),
ghost_record as (
    select
        '!UNKNOWN'::text as work_id,
        '!UNKNOWN'::text as institution_id,
        '!UNKNOWN'::text as country_code,
        '!UNKNOWN'::text as display_name,
        '!UNKNOWN'::text as ror,
        '!UNKNOWN'::text as type,
        {{ dbt_date.today() }} as dv_load_datetime
)

select * from casted
union all
select * from ghost_record
  
