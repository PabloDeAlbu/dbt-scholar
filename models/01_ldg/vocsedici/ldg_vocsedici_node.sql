{% set node_relation = source('vocsedici', 'node') %}
{% set node_field_data_relation = source('vocsedici', 'node_field_data') %}
{% if execute %}
  {% set node_col_names = adapter.get_columns_in_relation(node_relation) | map(attribute='name') | map('lower') | list %}
  {% set node_field_data_col_names = adapter.get_columns_in_relation(node_field_data_relation) | map(attribute='name') | map('lower') | list %}
{% else %}
  {% set node_col_names = none %}
  {% set node_field_data_col_names = none %}
{% endif %}

with node_source as (
  select *
  from {{ node_relation }}
),
node_field_data_source as (
  select *
  from {{ node_field_data_relation }}
),
renamed as (
  select
    n.nid,
    n.vid,
    n.type,
    n.uuid::text as node_uuid,
    coalesce(nfd.langcode::text, n.langcode::text) as node_langcode,
    {{ safe_cast('nfd', 'status', alias='status', col_names=node_field_data_col_names, default_mode='null') }},
    {{ safe_cast('nfd', 'created', alias='created', col_names=node_field_data_col_names, default_mode='null') }},
    {{ safe_cast('nfd', 'changed', alias='changed', col_names=node_field_data_col_names, default_mode='null') }},
    n._load_datetime::timestamp as _load_datetime
  from node_source n
  left join node_field_data_source nfd
    on n.nid = nfd.nid
   and n.vid = nfd.vid
   and n.type = nfd.type
),
casted as (
  select
    nid::bigint as node_id,
    vid::bigint as revision_id,
    type::text as node_type,
    node_uuid::text as node_uuid,
    node_langcode::text as langcode,
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
    '!UNKNOWN'::text as node_uuid,
    '!UNKNOWN'::text as langcode,
    false as is_published,
    '1900-01-01'::timestamp as created_at,
    '1900-01-01'::timestamp as changed_at,
    {{ dbt_date.today() }} as _load_datetime
)

select * from casted
union all
select * from ghost_record
