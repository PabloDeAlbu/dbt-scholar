{% set researchproduct_originalid_relation = source('openaire', 'researchproduct_originalid') %}
{% if execute %}
  {% set researchproduct_originalid_col_names = adapter.get_columns_in_relation(researchproduct_originalid_relation) | map(attribute='name') | map('lower') | list %}
{% else %}
  {% set researchproduct_originalid_col_names = none %}
{% endif %}

with source as (
  select * from {{ researchproduct_originalid_relation }}
),
base as (
  select 
    id::text as researchproduct_id,
    {{ safe_cast(researchproduct_originalid_relation, 'originalIds', 'text', alias='original_id', col_names=researchproduct_originalid_col_names) }},
    dv_load_datetime::timestamp
 from source
),
ghost_record as (
  select
    '!UNKNOWN'::text as researchproduct_id,
    '!UNKNOWN'::text as original_id,
    {{ dbt_date.today() }} as dv_load_datetime
)

select * from base
union all
select * from ghost_record
