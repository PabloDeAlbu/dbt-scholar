{% set paragraph_relation = source('vocsedici', 'paragraphs_item_field_data') %}
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
    langcode as paragraph_langcode,
    {{ safe_cast(paragraph_relation, 'status', alias='status', col_names=paragraph_col_names) }},
    {{ safe_cast(paragraph_relation, 'created', alias='created', col_names=paragraph_col_names) }},
    {{ safe_cast(paragraph_relation, 'parent_id', alias='parent_id', col_names=paragraph_col_names) }},
    {{ safe_cast(paragraph_relation, 'parent_type', 'text', alias='parent_type', col_names=paragraph_col_names, default_mode='ghost') }},
    {{ safe_cast(paragraph_relation, 'parent_field_name', 'text', alias='parent_field_name', col_names=paragraph_col_names, default_mode='ghost') }},
    {{ safe_cast(paragraph_relation, '_load_datetime', 'timestamp', alias='_load_datetime', col_names=paragraph_col_names, default_mode='ghost') }}
  from source
),
casted as (
  select
    id::bigint as paragraph_id,
    revision_id::bigint as revision_id,
    type::text as paragraph_type,
    paragraph_langcode::text as langcode,
    case
      when lower(coalesce(status::text, '0')) in ('1', 't', 'true', 'y', 'yes') then true
      else false
    end as is_published,
    case
      when created::text ~ '^[0-9]+$' then to_timestamp(created::double precision)
      when nullif(created::text, '') is null then null
      else created::text::timestamp
    end as created_at,
    parent_id::bigint as parent_id,
    parent_type::text as parent_type,
    parent_field_name::text as parent_field_name,
    _load_datetime::timestamp as _load_datetime
  from renamed
),
ghost_record as (
  select
    -1::bigint as paragraph_id,
    -1::bigint as revision_id,
    '!UNKNOWN'::text as paragraph_type,
    '!UNKNOWN'::text as langcode,
    false as is_published,
    '1900-01-01'::timestamp as created_at,
    -1::bigint as parent_id,
    '!UNKNOWN'::text as parent_type,
    '!UNKNOWN'::text as parent_field_name,
    {{ dbt_date.today() }} as _load_datetime
)

select * from casted
union all
select * from ghost_record
