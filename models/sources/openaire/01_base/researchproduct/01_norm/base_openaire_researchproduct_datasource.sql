{%- set columns = dbt_utils.get_filtered_columns_in_relation(from=source('openaire', 'researchproduct')) -%}

with source as (
    select * from {{ source('openaire', 'researchproduct') }}
  ),
  renamed as (
      select
        {% if 'relCollectedFromDatasourceId' in columns -%}
          {{ adapter.quote("id") }}::varchar as researchproduct_id,
          {{ adapter.quote("relCollectedFromDatasourceId") }}::varchar as datasource_id,
          {{ dbt_date.convert_timezone("load_datetime") }} as load_datetime
        {% else %}
          null as researchproduct_id,
          null as datasource_id,
          null as load_datetime
        {% endif %}
      from source
        {% if 'relCollectedFromDatasourceId' not in columns -%}
          limit 1
        {% endif %}
  )
  select * from renamed
