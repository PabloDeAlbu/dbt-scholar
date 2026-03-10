{% set map_researchproduct_author_relation = source('openaire', 'map_researchproduct_author') %}
{% if execute %}
  {% set map_researchproduct_author_col_names = adapter.get_columns_in_relation(map_researchproduct_author_relation) | map(attribute='name') | map('lower') | list %}
{% else %}
  {% set map_researchproduct_author_col_names = none %}
{% endif %}

with source as (
  select * from {{ map_researchproduct_author_relation }}
),
casted as (
  select
    id::text as researchproduct_id,
    name::text,
    rank::int,
    surname::text,
    {{ safe_cast(map_researchproduct_author_relation, 'fullName', 'text', alias='full_name', col_names=map_researchproduct_author_col_names, default_mode='ghost') }},
    {{ safe_cast(map_researchproduct_author_relation, 'pid.id.scheme', 'text', alias='pid_scheme', col_names=map_researchproduct_author_col_names, default_mode='ghost') }},
    {{ safe_cast(map_researchproduct_author_relation, 'pid.id.value', 'text', alias='orcid', col_names=map_researchproduct_author_col_names, default_mode='ghost') }},
    {{ safe_cast(map_researchproduct_author_relation, 'pid.provenance', 'text', alias='pid_provenance', col_names=map_researchproduct_author_col_names, default_mode='ghost') }},
    _load_datetime::timestamp
  from source
),
ghost_record as (
  select
    '!UNKNOWN'::text as researchproduct_id,
    '!UNKNOWN'::text as name,
    -1::int as rank,
    '!UNKNOWN'::text as surname,
    '!UNKNOWN'::text as full_name,
    '!UNKNOWN'::text as pid_scheme,
    '!UNKNOWN'::text as orcid,
    '!UNKNOWN'::text as pid_provenance,
    {{ dbt_date.today() }} as _load_datetime
)

SELECT * FROM casted
union all
select * from ghost_record
