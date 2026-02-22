{% set researchproduct_relation = source('openaire', 'researchproduct') %}
{% if execute %}
  {% set researchproduct_col_names = adapter.get_columns_in_relation(researchproduct_relation) | map(attribute='name') | map('lower') | list %}
{% else %}
  {% set researchproduct_col_names = none %}
{% endif %}

with source as (
  select * from {{ researchproduct_relation }}
),
renamed as (
  select
    id::text as researchproduct_id,
    {{ safe_cast(researchproduct_relation, 'openAccessColor', 'text', alias='open_access_color', col_names=researchproduct_col_names) }},
    {{ safe_cast(researchproduct_relation, 'publiclyFunded', 'text', alias='publicly_funded', col_names=researchproduct_col_names) }},
    type::text as type,
    {{ safe_cast(researchproduct_relation, 'mainTitle', 'text', alias='main_title', col_names=researchproduct_col_names) }},
    {{ safe_cast(researchproduct_relation, 'publicationDate', 'timestamp', alias='publication_date', col_names=researchproduct_col_names) }},
    {{ safe_cast(researchproduct_relation, 'publisher', 'text', alias='publisher', col_names=researchproduct_col_names) }},
    {{ safe_cast(researchproduct_relation, 'embargoEndDate', 'timestamp', alias='embargo_end_date', col_names=researchproduct_col_names) }},
    {{ safe_cast(researchproduct_relation, 'isGreen', 'boolean', alias='is_green', col_names=researchproduct_col_names) }},
    {{ safe_cast(researchproduct_relation, 'isInDiamondJournal', 'boolean', alias='is_in_diamond_journal', col_names=researchproduct_col_names) }},
    language_code::text as language_code,
    language_label::text as language_label,
    {{ safe_cast(researchproduct_relation, 'bestAccessRight_label', 'text', alias='best_access_right', col_names=researchproduct_col_names) }},
    {{ safe_cast(researchproduct_relation, 'bestAccessRight_scheme', 'text', alias='best_access_right_uri', col_names=researchproduct_col_names) }},
    {{ safe_cast(researchproduct_relation, 'citationImpact.citationClass', 'text', alias='citation_class', col_names=researchproduct_col_names) }},
    {{ safe_cast(researchproduct_relation, 'citationImpact.citationCount', 'int', alias='citation_count', col_names=researchproduct_col_names) }},
    {{ safe_cast(researchproduct_relation, 'citationImpact.impulse', 'float', alias='impulse', col_names=researchproduct_col_names) }},
    {{ safe_cast(researchproduct_relation, 'citationImpact.impulseClass', 'text', alias='impulse_class', col_names=researchproduct_col_names) }},
    {{ safe_cast(researchproduct_relation, 'citationImpact.influence', 'float', alias='influence', col_names=researchproduct_col_names) }},
    {{ safe_cast(researchproduct_relation, 'citationImpact.influenceClass', 'text', alias='influence_class', col_names=researchproduct_col_names) }},
    {{ safe_cast(researchproduct_relation, 'citationImpact.popularity', 'float', alias='popularity', col_names=researchproduct_col_names) }},
    {{ safe_cast(researchproduct_relation, 'citationImpact.popularityClass', 'text', alias='popularity_class', col_names=researchproduct_col_names) }},
    {{ safe_cast(researchproduct_relation, 'usageCounts.downloads', 'int', alias='downloads', col_names=researchproduct_col_names) }},
    {{ safe_cast(researchproduct_relation, 'usageCounts.views', 'int', alias='views', col_names=researchproduct_col_names) }},
    dv_load_datetime::timestamp
  from source
),
ghost_record as (
  select
    '!UNKNOWN'::text as researchproduct_id,
    '!UNKNOWN'::text as open_access_color,
    '!UNKNOWN'::text as publicly_funded,
    '!UNKNOWN'::text as type,
    '!UNKNOWN'::text as main_title,
    '1900-01-01'::timestamp as publication_date,
    '!UNKNOWN'::text as publisher,
    '1900-01-01'::timestamp as embargo_end_date,
    false as is_green,
    false as is_in_diamond_journal,
    '!UNKNOWN'::text as language_code,
    '!UNKNOWN'::text as language_label,
    '!UNKNOWN'::text as best_access_right,
    '!UNKNOWN'::text as best_access_right_uri,
    '!UNKNOWN'::text as citation_class,
    -1::int as citation_count,
    -1.0::float as impulse,
    '!UNKNOWN'::text as impulse_class,
    -1.0::float as influence,
    '!UNKNOWN'::text as influence_class,
    -1.0::float as popularity,
    '!UNKNOWN'::text as popularity_class,
    -1::int as downloads,
    -1::int as views,
    {{ dbt_date.today() }} as dv_load_datetime
)

select * from renamed
union all
select * from ghost_record
