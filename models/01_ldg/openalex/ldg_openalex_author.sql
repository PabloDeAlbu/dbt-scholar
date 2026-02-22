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
        dv_load_datetime   
    from {{ source('openalex', 'author') }}
),

casted as (
    select
        author_id::text,
        orcid::text,
        display_name::text,
        works_count::text,
        cited_by_count::int,
        updated_date::timestamp,
        created_date::timestamp,
        dv_load_datetime::timestamp
    from source
),
ghost_record as (
    select
        '!UNKNOWN'::text as author_id,
        '!UNKNOWN'::text as orcid,
        '!UNKNOWN'::text as display_name,
        '!UNKNOWN'::text as works_count,
        -1::int as cited_by_count,
        '1900-01-01'::timestamp as updated_date,
        '1900-01-01'::timestamp as created_date,
        {{ dbt_date.today() }} as dv_load_datetime
)

select * from casted
union all
select * from ghost_record
