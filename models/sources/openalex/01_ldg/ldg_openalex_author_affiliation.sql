with source as (
      select * from {{ source('openalex', 'map_author_affiliation') }}
),
renamed as (
    select  
        id as author_id,
        institution_id,
        years, 
        load_datetime
    from source
)
,
casted as (
    select  
        author_id::varchar,
        institution_id::varchar,
        years::int,
        {{ dbt_date.convert_timezone("load_datetime") }} as load_datetime
    from renamed
)
select * from casted
  