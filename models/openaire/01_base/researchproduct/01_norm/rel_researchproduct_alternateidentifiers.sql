with source as (
        select * from {{ source('openaire', 'rel_researchproduct_alternateidentifiers') }}
  ),
  renamed as (
      select
        id::varchar,
        scheme::varchar,
        value::varchar
      from source
  )
  select * from renamed
    