with source as (
        select * from {{ source('openaire', 'rel_researchproduct_originalid') }}
  ),
  renamed as (
      select
        COALESCE(id::varchar, 'NO DATA') as researchproduct_id,
        COALESCE({{ adapter.quote("originalIds") }}::varchar, 'NO DATA') as original_id,
        load_datetime::timestamp as load_datetime
      from source
  )
  select * from renamed
