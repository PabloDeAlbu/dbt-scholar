{% set paragraph_relation = source('vocsedici', 'paragraphs_item') %}
{% if execute %}
  {% set paragraph_col_names = adapter.get_columns_in_relation(paragraph_relation) | map(attribute='name') | map('lower') | list %}
{% else %}
  {% set paragraph_col_names = none %}
{% endif %}

with source as (
  select *
  from {{ paragraph_relation }}
),
renamed as (
  select
    id,
    revision_id,
    type,
    {{ safe_cast(paragraph_relation, 'uuid', 'text', alias='paragraph_uuid', col_names=paragraph_col_names, default_mode='ghost') }},
    {{ safe_cast(paragraph_relation, 'langcode', 'text', alias='paragraph_langcode', col_names=paragraph_col_names, default_mode='ghost') }},
    {{ safe_cast(paragraph_relation, '_load_datetime', 'timestamp', alias='_load_datetime', col_names=paragraph_col_names, default_mode='ghost') }}
  from source
),
casted as (
  select
    id::bigint as paragraph_id,
    revision_id::bigint as revision_id,
    type::text as paragraph_type,
    paragraph_uuid::text as paragraph_uuid,
    paragraph_langcode::text as langcode,
    _load_datetime::timestamp as _load_datetime
  from renamed
),
ghost_record as (
  select
    -1::bigint as paragraph_id,
    -1::bigint as revision_id,
    '!UNKNOWN'::text as paragraph_type,
    '!UNKNOWN'::text as paragraph_uuid,
    '!UNKNOWN'::text as langcode,
    {{ dbt_date.today() }} as _load_datetime
)

select * from casted
union all
select * from ghost_record
