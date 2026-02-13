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
),
ghost_record as (
    select
        '!UNKNOWN'::text as author_id,
        '!UNKNOWN'::text as count,
        '!UNKNOWN'::text as topic_id,
        '!UNKNOWN'::text as domain_id,
        '!UNKNOWN'::text as field_id,
        '!UNKNOWN'::text as subfield_id,
        {{ dbt_date.today() }} as load_datetime
)


select * from casted
union all
select * from ghost_record
