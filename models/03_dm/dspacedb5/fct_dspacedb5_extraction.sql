{{ config(
    materialized='incremental',
    unique_key='extract_cdk',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
    indexes=[
        {'columns': ['extract_cdk'], 'unique': true},
        {'columns': ['institution_ror', 'source_label', 'base_url']},
        {'columns': ['load_datetime']}
    ],
    post_hook=[
        "analyze {{ this }}"
    ]
) }}

WITH base AS (
    SELECT
        extract_cdk,
        scope_hk,
        source_label,
        institution_ror,
        base_url,
        extract_datetime,
        load_datetime,
        source
    FROM {{ ref('sat_dspacedb5__extract') }}
    {% if is_incremental() %}
    WHERE load_datetime > (SELECT COALESCE(MAX(load_datetime), '1900-01-01'::timestamp) FROM {{ this }})
    {% endif %}
)

SELECT * FROM base
