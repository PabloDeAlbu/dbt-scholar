{% set researchproduct_authors_relation = source('openaire', 'researchproduct_authors') %}
{% if execute %}
  {% set researchproduct_authors_col_names = adapter.get_columns_in_relation(researchproduct_authors_relation) | map(attribute='name') | map('lower') | list %}
{% else %}
  {% set researchproduct_authors_col_names = none %}
{% endif %}

with source as (
  select * from {{ researchproduct_authors_relation }}
),
casted as (
  select
    id::text as researchproduct_id,
    name::text,
    rank::int,
    surname::text,
    {{ safe_cast(researchproduct_authors_relation, 'fullName', 'text', alias='full_name', col_names=researchproduct_authors_col_names) }},
    {{ safe_cast(researchproduct_authors_relation, 'pid.id.scheme', 'text', alias='pid_scheme', col_names=researchproduct_authors_col_names) }},
    {{ safe_cast(researchproduct_authors_relation, 'pid.id.value', 'text', alias='orcid', col_names=researchproduct_authors_col_names) }},
    {{ safe_cast(researchproduct_authors_relation, 'pid.provenance', 'text', alias='pid_provenance', col_names=researchproduct_authors_col_names) }},
    dv_load_datetime::timestamp
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
    {{ dbt_date.today() }} as dv_load_datetime
)

SELECT * FROM casted
union all
select * from ghost_record
