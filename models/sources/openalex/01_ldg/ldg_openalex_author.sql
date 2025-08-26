{{ config(materialized='table')}}

with source as (
    select 
        id as author_id,
        orcid,
        display_name,
        works_count,
        cited_by_count,
        updated_date,
        created_date,
        load_datetime   
    from {{ source('openalex', 'author') }}
),

casted as (
    select
        author_id::varchar,
        orcid::varchar,
        display_name::varchar,
        works_count::varchar,
        cited_by_count::int,
        updated_date::timestamp,
        created_date::timestamp,
        load_datetime::timestamp
    from source
)

select * from casted
