{{ config(materialized='table')}}

with source as (
      select * from {{ source('openalex', 'author') }}
),

renamed as (
    select
        {{ adapter.quote("id") }} as author_id,
        {{ adapter.quote("orcid") }},
        {{ adapter.quote("display_name") }},
        {{ adapter.quote("works_count") }},
        {{ adapter.quote("cited_by_count") }},
        {{ adapter.quote("updated_date") }},
        {{ adapter.quote("created_date") }},
        {{ adapter.quote("load_datetime") }}
    from source
),

casted as (
    select
        author_id::varchar,
        orcid::varchar,
        display_name::varchar,
        works_count::int,
        cited_by_count::int,
        {{ dbt_date.convert_timezone("updated_date") }} as updated_date,
        {{ dbt_date.convert_timezone("created_date") }} as created_date,
        {{ dbt_date.convert_timezone("load_datetime") }} as load_datetime
    from renamed
),

transformed as (
    select
        author_id,
        split_part(orcid, 'https://orcid.org/', 2) as orcid,
        display_name,
        works_count,
        cited_by_count,
        updated_date,
        created_date,
        load_datetime
    from casted
)

select * from transformed