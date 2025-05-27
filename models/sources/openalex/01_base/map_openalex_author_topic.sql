with source as (
      select * from {{ source('openalex', 'map_author_topic') }}
),
renamed as (
    select
        {{ adapter.quote("id") }},
        {{ adapter.quote("count") }},
        {{ adapter.quote("id_topic") }},
        {{ adapter.quote("domain_id") }},
        {{ adapter.quote("field_id") }},
        {{ adapter.quote("subfield_id") }},
        {{ adapter.quote("load_datetime") }}
    from source
)


select * from renamed
  