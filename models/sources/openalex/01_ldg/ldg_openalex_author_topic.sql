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
        author_id::text,
        count::text,
        topic_id::text,
        domain_id::text,
        field_id::text,
        subfield_id::text,
        load_datetime::timestamp
    from source
)


select * from casted
