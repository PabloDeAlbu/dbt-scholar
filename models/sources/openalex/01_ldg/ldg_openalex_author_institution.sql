with source as (
      select * from {{ source('openalex', 'map_author_institution') }}
),
renamed as (
    select  
        author_id,
        id as institution_id,
        country_code,
        display_name,
        ror,
        type, 
        load_datetime
    from source
)
,
casted as (
    select  
        author_id::varchar,
        institution_id::varchar,
        country_code::varchar,
        display_name::varchar,
        ror::varchar,
        type::varchar,
        {{ dbt_date.convert_timezone("load_datetime") }} as load_datetime
    from renamed
)
select * from casted
  