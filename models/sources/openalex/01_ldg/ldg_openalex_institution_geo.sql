with base as (
    select 
        {# FIXME #}
        geo::text,
        extract_datetime,
        load_datetime
    from {{ source('openalex', 'institution') }}
)


select * from base
