with base as (
    select
        id::text as work_id,
        display_name::text,
        topic_id::text,
        score::double precision,
        {{ adapter.quote("domain.display_name") }}::text as domain_display_name,
        {{ adapter.quote("domain.id") }}::text as domain_id,
        {{ adapter.quote("field.display_name") }}::text as field_display_name,
        {{ adapter.quote("field.id") }}::text as field_id,
        {{ adapter.quote("subfield.display_name") }}::text as subfield_display_name,
        {{ adapter.quote("subfield.id")}}::text as subfield_id,
        load_datetime::timestamp
    from {{ source('openalex', 'map_work_topics') }}
)
select * from base
