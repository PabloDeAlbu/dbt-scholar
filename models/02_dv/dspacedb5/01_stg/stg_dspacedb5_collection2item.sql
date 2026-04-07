{{ config(materialized='view') }}

WITH source AS (
    SELECT
        collection_item_id,
        collection_id,
        item_id,
        _source_label AS source_label,
        _institution_ror AS institution_ror,
        _extract_datetime AS extract_datetime,
        _load_datetime AS load_datetime,
        _institution_ror || '||' || _source_label || '||' || collection_id::text AS collection_bk,
        _institution_ror || '||' || _source_label || '||' || item_id::text AS item_bk,
        _institution_ror || '||' || _source_label || '||' || collection_item_id::text AS collection_item_bk
    FROM {{ ref('ldg_dspacedb5_collection2item') }}
),
final AS (
    SELECT
        collection_hk,
        item_hk,
        collection_item_hk,
        collection_bk,
        item_bk,
        collection_item_bk,
        collection_item_id,
        collection_id,
        item_id,
        source_label,
        institution_ror,
        extract_datetime,
        load_datetime
    FROM source s0
    CROSS JOIN LATERAL (
        SELECT
            {{ automate_dv.hash(columns='collection_bk', alias='collection_hk') }},
            {{ automate_dv.hash(columns='item_bk', alias='item_hk') }},
            {{ automate_dv.hash(columns='collection_item_bk', alias='collection_item_hk') }}
    ) s1
)

SELECT * FROM final
