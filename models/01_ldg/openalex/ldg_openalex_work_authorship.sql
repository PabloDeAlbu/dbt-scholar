{% set map_work_authorship_relation = source('openalex', 'map_work_author') %}
{% if execute %}
  {% set map_work_authorship_col_names = adapter.get_columns_in_relation(map_work_authorship_relation) | map(attribute='name') | map('lower') | list %}
{% else %}
  {% set map_work_authorship_col_names = none %}
{% endif %}

with source as (
    select
        work_id,
        author_id,
        {{ safe_cast(map_work_authorship_relation, 'author_display_name', 'text', alias='author_display_name', col_names=map_work_authorship_col_names, default_mode='ghost') }},
        {{ safe_cast(map_work_authorship_relation, 'author_orcid', 'text', alias='author_orcid', col_names=map_work_authorship_col_names, default_mode='ghost') }},
        author_position,
        _load_datetime
    from {{ map_work_authorship_relation }}
),

normalized as (
    select
        {{ normalize_text('work_id') }} as work_id,
        {{ normalize_text('author_id') }} as author_id,
        {{ normalize_text('author_display_name') }} as author_display_name,
        {{ normalize_text('author_orcid') }} as author_orcid,
        {{ normalize_text('author_position') }} as author_position,
        CONCAT_WS(
            '||',
            {{ normalize_text('author_id') }},
            {{ normalize_text('author_display_name') }},
            {{ normalize_text('author_orcid') }},
            {{ normalize_text('author_position') }}
        ) as authorship_id,
        _load_datetime::timestamp
    from source
),

deduplicated as (
    select distinct
        work_id,
        author_id,
        author_display_name,
        author_orcid,
        author_position,
        authorship_id,
        _load_datetime
    from normalized
),

ghost_record as (
    select
        '!UNKNOWN'::text as work_id,
        '!UNKNOWN'::text as author_id,
        '!UNKNOWN'::text as author_display_name,
        '!UNKNOWN'::text as author_orcid,
        '!UNKNOWN'::text as author_position,
        CONCAT_WS(
            '||',
            '!UNKNOWN'::text,
            '!UNKNOWN'::text,
            '!UNKNOWN'::text,
            '!UNKNOWN'::text
        ) as authorship_id,
        {{ dbt_date.today() }} as _load_datetime
)

select * from deduplicated
union all
select * from ghost_record
