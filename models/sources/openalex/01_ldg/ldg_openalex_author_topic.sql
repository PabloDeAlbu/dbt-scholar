with source as (
    select 
        id as author_id,
        count,
        id_topic as topic_id,
        domain_id,
        field_id,
        subfield_id,
        load_datetime
    from {{ source('openalex', 'map_author_topic') }}
),
casted as (
    select
        author_id::varchar,
        count::varchar,
        topic_id::varchar,
        domain_id::varchar,
        field_id::varchar,
        subfield_id::varchar,
        load_datetime::timestamp
    from source
)


select * from casted
