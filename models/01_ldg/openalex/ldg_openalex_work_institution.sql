with source as (
    select
        work_id,
        id as institution_id,
        country_code,
        display_name,
        ror,
        type,
        load_datetime
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
       load_datetime::timestamp
    from source
)

select * from casted
  