with source as (
        select * from {{ source('openaire', 'rel_researchproduct_originalid') }}
  ),
  renamed as (
      select
        id::varchar as researchproduct_id,
        {{ adapter.quote("originalIds") }}::varchar as original_id,
        {{ dbt_date.convert_timezone("load_datetime") }} as load_datetime
      from source
  )
  select * from renamed
