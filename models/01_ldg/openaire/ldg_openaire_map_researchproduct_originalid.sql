{% set map_researchproduct_originalid_relation = source('openaire', 'map_researchproduct_originalid') %}
{% if execute %}
  {% set map_researchproduct_originalid_col_names = adapter.get_columns_in_relation(map_researchproduct_originalid_relation) | map(attribute='name') | map('lower') | list %}
{% else %}
  {% set map_researchproduct_originalid_col_names = none %}
{% endif %}

with source as (
  select * from {{ map_researchproduct_originalid_relation }}
),
base as (
  select 
    id::text as researchproduct_id,
    {{ safe_cast(map_researchproduct_originalid_relation, 'originalIds', 'text', alias='original_id', col_names=map_researchproduct_originalid_col_names, default_mode='ghost') }},
    _load_datetime::timestamp
 from source
),
ghost_record as (
  select
    '!UNKNOWN'::text as researchproduct_id,
    '!UNKNOWN'::text as original_id,
    {{ dbt_date.today() }} as _load_datetime
)

select * from base
union all
select * from ghost_record
