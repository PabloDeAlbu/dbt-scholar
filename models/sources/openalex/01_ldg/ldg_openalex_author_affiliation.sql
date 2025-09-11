with source as (
    select 
        id as author_id,
        institution_id,
        years, 
        load_datetime
      from {{ source('openalex', 'map_author_affiliation') }}
),

casted as (
    select  
        author_id::text,
        institution_id::text,
        years::int,
        load_datetime::timestamp
    from source
)
select * from casted
  