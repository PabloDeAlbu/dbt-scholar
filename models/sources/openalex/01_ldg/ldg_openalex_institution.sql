with source as (
        select distinct             
            id as institution_id,
            country_code,
            display_name,
            ror,
            type as institution_type,
            load_datetime
    from {{ source('openalex', 'map_work_institution') }}
),

casted as (
    select
        institution_id::text,
        country_code::text,
        display_name::text,
        ror::text,
        institution_type::text,
        load_datetime::timestamp as load_datetime
    from source
)

select * from casted
  