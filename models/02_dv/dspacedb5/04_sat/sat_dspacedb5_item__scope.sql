{{ config(materialized='incremental') }}

WITH staged_source AS (
    SELECT
        item_hk,
        item_id,
        source_label,
        institution_ror,
        base_url,
        owningcollection_hk,
        item_scope_hashdiff AS hashdiff,
        effective_from,
        load_datetime,
        source
    FROM {{ ref('stg_dspacedb5_item') }}
    WHERE item_hk IS NOT NULL
),
{% if is_incremental() %}
existing_records AS (
    SELECT
        item_hk,
        hashdiff
    FROM {{ this }}
),
{% endif %}
unique_source_records AS (
    SELECT DISTINCT
        ss.item_hk,
        ss.hashdiff,
        ss.item_id,
        ss.source_label,
        ss.institution_ror,
        ss.base_url,
        ss.owningcollection_hk,
        ss.effective_from,
        ss.load_datetime,
        ss.source
    FROM staged_source AS ss
    {% if is_incremental() %}
    LEFT JOIN existing_records AS er
        USING (item_hk, hashdiff)
    WHERE er.item_hk IS NULL
    {% endif %}
)

SELECT *
FROM unique_source_records
