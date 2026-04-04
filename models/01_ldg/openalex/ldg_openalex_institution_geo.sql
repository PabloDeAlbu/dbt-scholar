with source as (
    select 
        * 
    from {{ source('openalex', 'institution') }}
),
base as (
    select 
        {# FIXME #}
        geo::text,
        _extract_datetime::timestamp as extract_datetime,
        _load_datetime::timestamp
    from source
),
ghost_record as (
    select
        '!UNKNOWN'::text as geo,
        '1900-01-01'::timestamp as extract_datetime,
        {{ dbt_date.today() }} as _load_datetime
)

select * from base
union all
select * from ghost_record
