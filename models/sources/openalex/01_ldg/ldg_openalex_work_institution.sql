with source as (
    select
        work_id,
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
        work_id::varchar,
        country_code::varchar,
        display_name::varchar,
        ror::varchar,
        institution_type::varchar,
       load_datetime::timestamp
    from source
)

select * from casted
  