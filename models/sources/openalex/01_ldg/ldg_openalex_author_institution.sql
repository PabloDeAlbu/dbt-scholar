with source as (
    select
        author_id,
        id as institution_id,
        load_datetime
    from {{ source('openalex', 'map_author_institution') }}
),
casted as (
    select  
        author_id::text,
        institution_id::text,
        load_datetime::timestamp
    from source
)
select * from casted
  