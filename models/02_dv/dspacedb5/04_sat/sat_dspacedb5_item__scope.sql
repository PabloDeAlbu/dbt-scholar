{{ config(materialized='incremental') }}

WITH staged_source AS (
    SELECT
        item_hk,
        item_id,
        source_label,
        institution_ror,
        base_url,
        owningcollection_hk,
        effective_from,
        load_datetime,
        source
    FROM {{ ref('stg_dspacedb5_item') }}
    WHERE item_hk IS NOT NULL
),
hashed_source AS (
    SELECT
        s0.item_hk,
        s0.item_id,
        s0.source_label,
        s0.institution_ror,
        s0.base_url,
        s0.owningcollection_hk,
        s0.effective_from,
        s0.load_datetime,
        s0.source,
        s1.hashdiff
    FROM staged_source AS s0
    CROSS JOIN LATERAL (
        SELECT {{ automate_dv.hash(
            columns=[
                'item_hk',
                'item_id',
                'source_label',
                'institution_ror',
                'base_url',
                'owningcollection_hk'
            ],
            alias='hashdiff',
            is_hashdiff=true
        ) }}
    ) AS s1
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
        hs.item_hk,
        hs.hashdiff,
        hs.item_id,
        hs.source_label,
        hs.institution_ror,
        hs.base_url,
        hs.owningcollection_hk,
        hs.effective_from,
        hs.load_datetime,
        hs.source
    FROM hashed_source AS hs
    {% if is_incremental() %}
    LEFT JOIN existing_records AS er
        USING (item_hk, hashdiff)
    WHERE er.item_hk IS NULL
    {% endif %}
)

SELECT *
FROM unique_source_records
