{% set node_field_data_relation = source('vocsedici', 'node_field_data') %}
{% if execute %}
  {% set node_field_data_col_names = adapter.get_columns_in_relation(node_field_data_relation) | map(attribute='name') | map('lower') | list %}
{% else %}
  {% set node_field_data_col_names = none %}
{% endif %}

with source as (
  select *
  from {{ node_field_data_relation }}
),
renamed as (
  select
    nid,
    vid,
    type,
    langcode as node_langcode,
    {{ safe_cast(node_field_data_relation, 'title', 'text', alias='title', col_names=node_field_data_col_names, default_mode='ghost') }},
    {{ safe_cast(node_field_data_relation, 'status', alias='status', col_names=node_field_data_col_names) }},
    {{ safe_cast(node_field_data_relation, 'created', alias='created', col_names=node_field_data_col_names) }},
    {{ safe_cast(node_field_data_relation, 'changed', alias='changed', col_names=node_field_data_col_names) }},
    {{ safe_cast(node_field_data_relation, '_load_datetime', 'timestamp', alias='_load_datetime', col_names=node_field_data_col_names, default_mode='ghost') }}
  from source
),
casted as (
  select
    nid::bigint as node_id,
    vid::bigint as revision_id,
    type::text as node_type,
    node_langcode::text as langcode,
    title::text as title,
    case
      when lower(coalesce(status::text, '0')) in ('1', 't', 'true', 'y', 'yes') then true
      else false
    end as is_published,
    case
      when created::text ~ '^[0-9]+$' then to_timestamp(created::double precision)
      when nullif(created::text, '') is null then null
      else created::text::timestamp
    end as created_at,
    case
      when changed::text ~ '^[0-9]+$' then to_timestamp(changed::double precision)
      when nullif(changed::text, '') is null then null
      else changed::text::timestamp
    end as changed_at,
    _load_datetime::timestamp as _load_datetime
  from renamed
),
ghost_record as (
  select
    -1::bigint as node_id,
    -1::bigint as revision_id,
    '!UNKNOWN'::text as node_type,
    '!UNKNOWN'::text as langcode,
    '!UNKNOWN'::text as title,
    false as is_published,
    '1900-01-01'::timestamp as created_at,
    '1900-01-01'::timestamp as changed_at,
    {{ dbt_date.today() }} as _load_datetime
)

select * from casted
union all
select * from ghost_record
