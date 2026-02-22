with source as (
    select
        author_id,
        id as institution_id,
        dv_load_datetime
    from {{ source('openalex', 'map_author_institution') }}
),
casted as (
    select  
        author_id::text,
        institution_id::text,
        dv_load_datetime::timestamp
    from source
),
ghost_record as (
    select
        '!UNKNOWN'::text as author_id,
        '!UNKNOWN'::text as institution_id,
        {{ dbt_date.today() }} as dv_load_datetime
)
select * from casted
union all
select * from ghost_record
  
