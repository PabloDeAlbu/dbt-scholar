{{ config(materialized='view') }}

WITH source AS (
    SELECT
        community_collection_id,
        community_id,
        collection_id,
        _source_label AS source_label,
        _institution_ror AS institution_ror,
        _extract_datetime AS extract_datetime,
        _load_datetime AS load_datetime,
        _institution_ror || '||' || _source_label || '||' || community_id::text AS community_bk,
        _institution_ror || '||' || _source_label || '||' || collection_id::text AS collection_bk,
        _institution_ror || '||' || _source_label || '||' || community_collection_id::text AS community_collection_bk
    FROM {{ ref('ldg_dspacedb5_community2collection') }}
),
final AS (
    SELECT
        community_hk,
        collection_hk,
        community_collection_hk,
        community_bk,
        collection_bk,
        community_collection_bk,
        community_collection_id,
        community_id,
        collection_id,
        source_label,
        institution_ror,
        extract_datetime,
        load_datetime
    FROM source s0
    CROSS JOIN LATERAL (
        SELECT
            {{ automate_dv.hash(columns='community_bk', alias='community_hk') }},
            {{ automate_dv.hash(columns='collection_bk', alias='collection_hk') }},
            {{ automate_dv.hash(columns='community_collection_bk', alias='community_collection_hk') }}
    ) s1
)

SELECT * FROM final
