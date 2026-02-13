with source as (
    select 
        * 
    from {{ source('openalex', 'institution') }}
),
base as (
    select 
        {# FIXME #}
        geo::text,
        extract_datetime,
        load_datetime
    from source
),
ghost_record as (
    select
        '!UNKNOWN'::text as geo,
        '1900-01-01'::timestamp as extract_datetime,
        {{ dbt_date.today() }} as load_datetime
)

select * from base
union all
select * from ghost_record
