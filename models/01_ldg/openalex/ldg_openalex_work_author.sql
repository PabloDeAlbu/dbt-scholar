{% set map_work_author_relation = source('openalex', 'map_work_author') %}
{% if execute %}
  {% set map_work_author_col_names = adapter.get_columns_in_relation(map_work_author_relation) | map(attribute='name') | map('lower') | list %}
{% else %}
  {% set map_work_author_col_names = none %}
{% endif %}

with source as (
    select
        work_id,
        author_id,
        {{ safe_cast(map_work_author_relation, 'author_display_name', 'text', alias='author_display_name', col_names=map_work_author_col_names, default_mode='ghost') }},
        {{ safe_cast(map_work_author_relation, 'author_orcid', 'text', alias='author_orcid', col_names=map_work_author_col_names, default_mode='ghost') }},
        author_position,
        _load_datetime
 from {{ map_work_author_relation }}
),

casted as (
    select
        work_id::text,
        COALESCE(NULLIF(BTRIM(author_id::text), ''), '!UNKNOWN') AS author_id,
        COALESCE(NULLIF(BTRIM(author_display_name::text), ''), '!UNKNOWN') AS author_display_name,
        COALESCE(NULLIF(BTRIM(author_orcid::text), ''), '!UNKNOWN') AS author_orcid,
        COALESCE(NULLIF(BTRIM(author_position::text), ''), '!UNKNOWN') AS author_position,
        _load_datetime::timestamp
    from source
),
ghost_record as (
    select
        '!UNKNOWN'::text as work_id,
        '!UNKNOWN'::text as author_id,
        '!UNKNOWN'::text as author_display_name,
        '!UNKNOWN'::text as author_orcid,
        '!UNKNOWN'::text as author_position,
        {{ dbt_date.today() }} as _load_datetime
)

select * from casted
union all
select * from ghost_record
  
