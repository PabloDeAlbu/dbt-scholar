with source as (
      select * from {{ source('openalex', 'map_work_author') }}
),
renamed as (
    select
        work_id,
        author_id,
        author_position,
        load_datetime
    from source
),

casted as (
    select
        work_id::varchar,
        author_id::varchar,
        author_position::varchar,
        {{ dbt_date.convert_timezone("load_datetime") }} as load_datetime
    from renamed
),

fillna as (
    select
        work_id,
        author_id,
        author_position,
        load_datetime
    from casted
)

select * from fillna
  