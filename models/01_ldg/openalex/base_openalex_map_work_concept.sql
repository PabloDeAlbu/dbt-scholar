with source as (
        select * from {{ source('openalex', 'map_work_concept') }}
  ),
  renamed as (
      select
          {{ adapter.quote("id") }},
        {{ adapter.quote("_filter_param") }},
        {{ adapter.quote("_filter_value") }},
        {{ adapter.quote("_extract_datetime") }},
        {{ adapter.quote("display_name") }},
        {{ adapter.quote("concept_id") }},
        {{ adapter.quote("level") }},
        {{ adapter.quote("score") }},
        {{ adapter.quote("wikidata") }},
        {{ adapter.quote("_load_datetime") }}

      from source
  )
  select * from renamed
    