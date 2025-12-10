with source as (
    select
        work_id,
        author_id,
        author_position,
        load_datetime
 from {{ source('openalex', 'map_work_author') }}
),

casted as (
    select
        work_id::text,
        author_id::text,
        author_position::text,
        load_datetime::timestamp
    from source
)

select * from casted
  