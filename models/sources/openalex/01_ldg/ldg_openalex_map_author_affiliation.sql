with source as (
        select * from {{ source('openalex', 'map_author_affiliation') }}
),
renamed as (
    select
      {{ adapter.quote("id") }} as author_id,
      {{ adapter.quote("institution_id") }},
      {{ adapter.quote("years") }},
      {{ adapter.quote("load_datetime") }}
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
