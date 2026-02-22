with source as (
    select 
        id as author_id,
        institution_id,
        years, 
        dv_load_datetime
      from {{ source('openalex', 'author_institution_year') }}
),

casted as (
    select  
        author_id::text,
        institution_id::text,
        years::int,
        dv_load_datetime::timestamp
    from source
),
ghost_record as (
    select
        '!UNKNOWN'::text as author_id,
        '!UNKNOWN'::text as institution_id,
        -1::int as years,
        {{ dbt_date.today() }} as dv_load_datetime
)
select * from casted
union all
select * from ghost_record
  
