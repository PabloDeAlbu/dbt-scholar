with source as (
        select * from {{ source('openaire', 'rel_researchproduct_originalid') }}
  ),
  renamed as (
      select
          {{ adapter.quote("id") }},
        {{ adapter.quote("originalIds") }}

      from source
  )
  select * from renamed
    