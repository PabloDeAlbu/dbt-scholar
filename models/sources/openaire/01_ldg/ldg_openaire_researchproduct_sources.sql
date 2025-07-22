{%- set columns = dbt_utils.get_filtered_columns_in_relation(from=source('openaire', 'researchproduct')) -%}

with source as (
    select * from {{ source('openaire', 'rel_researchproduct_sources') }}
  ),
  renamed as (
      select *
      from source
  )
  select * from renamed
