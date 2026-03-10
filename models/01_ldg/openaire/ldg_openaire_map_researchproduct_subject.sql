{% set map_researchproduct_subject_relation = source('openaire', 'map_researchproduct_subject') %}
{% if execute %}
  {% set map_researchproduct_subject_col_names = adapter.get_columns_in_relation(map_researchproduct_subject_relation) | map(attribute='name') | map('lower') | list %}
{% else %}
  {% set map_researchproduct_subject_col_names = none %}
{% endif %}

with source as (
  select * from {{ map_researchproduct_subject_relation }}
),
renamed as (
  select
    id::text as researchproduct_id,
    provenance::text as provenance,
    {{ safe_cast(map_researchproduct_subject_relation, 'subject.scheme', 'text', alias='subject_scheme', col_names=map_researchproduct_subject_col_names, default_mode='ghost') }},
    {{ safe_cast(map_researchproduct_subject_relation, 'subject.value', 'text', alias='subject_value', col_names=map_researchproduct_subject_col_names, default_mode='ghost') }},
    _load_datetime::timestamp
  from source
),
ghost_record as (
  select
    '!UNKNOWN'::text as researchproduct_id,
    '!UNKNOWN'::text as provenance,
    '!UNKNOWN'::text as subject_scheme,
    '!UNKNOWN'::text as subject_value,
    {{ dbt_date.today() }} as _load_datetime
)

select * from renamed
where subject_scheme != 'keyword'
union all
select * from ghost_record
    
