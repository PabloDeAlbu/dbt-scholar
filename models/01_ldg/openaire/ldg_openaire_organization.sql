{% set organization_relation = source('openaire', 'organization') %}
{% if execute %}
  {% set organization_col_names = adapter.get_columns_in_relation(organization_relation) | map(attribute='name') | map('lower') | list %}
{% else %}
  {% set organization_col_names = none %}
{% endif %}

with source as (
  select * from {{ organization_relation }}
),
base as (
  select 
    organization_id::text,
    acronym::text,
    {{ safe_cast(organization_relation, 'legalName', 'text', alias='legalname', col_names=organization_col_names, default_mode='ghost') }},
    _load_datetime::timestamp
  from source
),
ghost_record as (
  select
    '!UNKNOWN'::text as organization_id,
    '!UNKNOWN'::text as acronym,
    '!UNKNOWN'::text as legalname,
    {{ dbt_date.today() }} as _load_datetime
)

select * from base
union all
select * from ghost_record
