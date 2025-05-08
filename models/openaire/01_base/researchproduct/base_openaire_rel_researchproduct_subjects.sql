with source as (
        select * from {{ source('openaire', 'rel_researchproduct_subjects') }}
  ),
  renamed as (
      select
          {{ adapter.quote("id") }},
        {{ adapter.quote("provenance") }},
        {{ adapter.quote("subject.scheme") }},
        {{ adapter.quote("subject.value") }}

      from source
  )
  select * from renamed
    