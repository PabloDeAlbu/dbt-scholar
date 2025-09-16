{%- set columns = dbt_utils.get_filtered_columns_in_relation(from=source('openaire', 'researchproduct')) -%}

with source as (
    select * from {{ source('openaire', 'researchproduct_collectedfrom') }}
  ),
  renamed as (
      select
        researchproduct_id::text,
        datasource_id::text,
        value::text,
        load_datetime::timestamp
      from source
  )
  select * from renamed
