with source as (
    select
        work_id,
        author_id,
        author_position,
        dv_load_datetime
 from {{ source('openalex', 'map_work_author') }}
),

casted as (
    select
        work_id::text,
        author_id::text,
        author_position::text,
        dv_load_datetime::timestamp
    from source
),
ghost_record as (
    select
        '!UNKNOWN'::text as work_id,
        '!UNKNOWN'::text as author_id,
        '!UNKNOWN'::text as author_position,
        {{ dbt_date.today() }} as dv_load_datetime
)

select * from casted
union all
select * from ghost_record
  
