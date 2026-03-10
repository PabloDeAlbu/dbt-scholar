with source as (
    select 
        id as author_id,
        institution_id,
        years, 
        _load_datetime
      from {{ source('openalex', 'map_author_institution_year') }}
),

casted as (
    select  
        author_id::text,
        institution_id::text,
        years::int,
        _load_datetime::timestamp
    from source
),
ghost_record as (
    select
        '!UNKNOWN'::text as author_id,
        '!UNKNOWN'::text as institution_id,
        -1::int as years,
        {{ dbt_date.today() }} as _load_datetime
)
select * from casted
union all
select * from ghost_record
  
