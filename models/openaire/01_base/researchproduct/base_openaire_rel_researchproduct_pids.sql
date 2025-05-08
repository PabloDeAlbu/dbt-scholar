with source as (
        select * from {{ source('openaire', 'rel_researchproduct_pids') }}
  ),
  renamed as (
      select
          {{ adapter.quote("id") }},
        {{ adapter.quote("scheme") }},
        {{ adapter.quote("value") }}

      from source
  )
  select * from renamed
    