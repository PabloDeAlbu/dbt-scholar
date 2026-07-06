{{ config(
    materialized='incremental',
    on_schema_change='append_new_columns'
) }}

WITH staged_source AS (
    SELECT
        scope_hk,
        extract_cdk,
        extract_hashdiff,
        scope_bk,
        institution_ror,
        source_label,
        base_url,
        extract_datetime,
        effective_from,
        load_datetime,
        source
    FROM {{ ref('stg_dspacedb5__scope') }}
    WHERE scope_bk IS NOT NULL
      AND extract_datetime IS NOT NULL
),
hashed_source AS (
    SELECT
        s0.scope_hk,
        s0.extract_cdk,
        s0.extract_hashdiff AS hashdiff,
        s0.institution_ror,
        s0.source_label,
        s0.base_url,
        s0.extract_datetime,
        s0.effective_from,
        s0.load_datetime,
        s0.source
    FROM staged_source AS s0
),
{% if is_incremental() %}
existing_records AS (
    SELECT extract_cdk
    FROM {{ this }}
),
{% endif %}
unique_source_records AS (
    SELECT DISTINCT
        hs.scope_hk,
        hs.extract_cdk,
        hs.hashdiff,
        hs.institution_ror,
        hs.source_label,
        hs.base_url,
        hs.extract_datetime,
        hs.effective_from,
        hs.load_datetime,
        hs.source
    FROM hashed_source AS hs
    {% if is_incremental() %}
    LEFT JOIN existing_records AS er
        USING (extract_cdk)
    WHERE er.extract_cdk IS NULL
    {% endif %}
)

SELECT *
FROM unique_source_records
