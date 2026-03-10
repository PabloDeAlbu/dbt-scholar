{{ config(materialized='view') }}

{% set work_relation = source('openalex', 'work') %}
{% if execute %}
  {% set work_col_names = adapter.get_columns_in_relation(work_relation) | map(attribute='name') | map('lower') | list %}
{% else %}
  {% set work_col_names = none %}
{% endif %}

WITH raw_source AS (
    SELECT
        id::text AS work_id,
        _filter_param::text AS _filter_param,
        _filter_value::text AS _filter_value,
        _load_datetime::timestamp AS load_datetime,
        {{ safe_cast(
            work_relation,
            '_extract_datetime',
            'timestamp',
            alias='extract_datetime',
            col_names=work_col_names,
            default_sql='_load_datetime'
        ) }}
    FROM {{ work_relation }}
),

final AS (
    SELECT
        work_hk,
        work_id,
        _filter_param,
        _filter_value,
        load_datetime,
        extract_datetime,
        source,
        {{ automate_dv.hash(
            columns=['work_hk', 'extract_datetime', '_filter_param', '_filter_value'],
            alias='extract_cdk'
        ) }},
        {{ automate_dv.hash(
            columns=['extract_datetime', '_filter_param', '_filter_value'],
            alias='work_extract_hashdiff',
            is_hashdiff=true
        ) }}
    FROM (
        SELECT
            {{ automate_dv.hash(columns='work_id', alias='work_hk') }},
            work_id,
            _filter_param,
            _filter_value,
            load_datetime,
            extract_datetime,
            '!OPENALEX' AS source
        FROM raw_source
    ) s
)

SELECT * FROM final
