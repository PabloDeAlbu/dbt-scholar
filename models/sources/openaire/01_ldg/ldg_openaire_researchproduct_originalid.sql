with source as (
        select * from {{ source('openaire', 'researchproduct_originalid') }}
  ),
  renamed as (
      select
        id::text as researchproduct_id,
        {{ adapter.quote("originalIds") }}::text as original_id,
        {{ dbt_date.convert_timezone("load_datetime") }} as load_datetime
      from source
  )
  select * from renamed
