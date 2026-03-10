{{ config(materialized='view') }}

{% set researchproduct_relation = source('openaire', 'researchproduct') %}
{% if execute %}
  {% set researchproduct_col_names = adapter.get_columns_in_relation(researchproduct_relation) | map(attribute='name') | map('lower') | list %}
{% else %}
  {% set researchproduct_col_names = none %}
{% endif %}

WITH raw_source AS (
    SELECT
        id::text AS researchproduct_id,
        _filter_param::text AS _filter_param,
        _filter_value::text AS _filter_value,
        _load_datetime::timestamp AS load_datetime,
        {{ safe_cast(
            researchproduct_relation,
            '_extract_datetime',
            'timestamp',
            alias='extract_datetime',
            col_names=researchproduct_col_names,
            default_sql='_load_datetime'
        ) }}
    FROM {{ researchproduct_relation }}
),

final AS (
    SELECT
        researchproduct_hk,
        researchproduct_id,
        _filter_param,
        _filter_value,
        load_datetime,
        extract_datetime,
        source,
        _filter_param_cdk,
        _filter_value_cdk,
        {{ automate_dv.hash(
            columns=['researchproduct_hk', 'extract_datetime', '_filter_param_cdk', '_filter_value_cdk'],
            alias='extract_cdk'
        ) }},
        {{ automate_dv.hash(
            columns=['_filter_param', '_filter_value', 'extract_datetime'],
            alias='researchproduct_extract_hashdiff',
            is_hashdiff=true
        ) }}
    FROM (
        SELECT
            {{ automate_dv.hash(columns='researchproduct_id', alias='researchproduct_hk') }},
            researchproduct_id,
            _filter_param,
            _filter_value,
            load_datetime,
            extract_datetime,
            '!OPENAIRE' AS source,
            COALESCE(_filter_param, '') AS _filter_param_cdk,
            COALESCE(_filter_value, '') AS _filter_value_cdk
        FROM raw_source
    ) s
)

SELECT * FROM final
