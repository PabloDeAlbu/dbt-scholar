{{ config(
    materialized='incremental',
    on_schema_change='append_new_columns'
) }}

WITH staged_source AS (
    SELECT
        scope_hk,
        extract_cdk,
        extract_hashdiff AS hashdiff,
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
{% if is_incremental() %}
existing_records AS (
    SELECT extract_cdk
    FROM {{ this }}
),
{% endif %}
unique_source_records AS (
    SELECT DISTINCT
        ss.scope_hk,
        ss.extract_cdk,
        ss.hashdiff,
        ss.institution_ror,
        ss.source_label,
        ss.base_url,
        ss.extract_datetime,
        ss.effective_from,
        ss.load_datetime,
        ss.source
    FROM staged_source AS ss
    {% if is_incremental() %}
    LEFT JOIN existing_records AS er
        USING (extract_cdk)
    WHERE er.extract_cdk IS NULL
    {% endif %}
)

SELECT *
FROM unique_source_records
