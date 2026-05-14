{% set field_relation = source('vocsedici', 'node__field_filiacion') %}
{% if execute %}
  {% set field_col_names = adapter.get_columns_in_relation(field_relation) | map(attribute='name') | map('lower') | list %}
{% else %}
  {% set field_col_names = none %}
{% endif %}

with source as (
  select *
  from {{ field_relation }}
),
renamed as (
  select
    bundle,
    deleted,
    entity_id,
    revision_id,
    langcode as field_langcode,
    delta as field_delta,
    {{ safe_cast(field_relation, 'field_filiacion_target_id', 'text', alias='affiliation_target_id', col_names=field_col_names, default_mode='ghost') }},
    {{ safe_cast(field_relation, 'field_filiacion_target_revision_id', 'bigint', alias='affiliation_target_revision_id', col_names=field_col_names, default_mode='ghost') }},
    {{ safe_cast(field_relation, '_load_datetime', 'timestamp', alias='_load_datetime', col_names=field_col_names, default_mode='ghost') }}
  from source
),
casted as (
  select
    bundle::text as bundle,
    case
      when lower(coalesce(deleted::text, '0')) in ('1', 't', 'true', 'y', 'yes') then true
      else false
    end as is_deleted,
    entity_id::bigint as node_id,
    revision_id::bigint as revision_id,
    field_langcode::text as langcode,
    field_delta::int as delta,
    affiliation_target_id::text as affiliation_target_id,
    affiliation_target_revision_id::bigint as affiliation_target_revision_id,
    _load_datetime::timestamp as _load_datetime
  from renamed
),
ghost_record as (
  select
    '!UNKNOWN'::text as bundle,
    false as is_deleted,
    -1::bigint as node_id,
    -1::bigint as revision_id,
    '!UNKNOWN'::text as langcode,
    -1::int as delta,
    '!UNKNOWN'::text as affiliation_target_id,
    -1::bigint as affiliation_target_revision_id,
    {{ dbt_date.today() }} as _load_datetime
)

select * from casted
union all
select * from ghost_record
