{{ config(materialized='view') }}

WITH source AS (
    SELECT
        community_id,
        logo_bitstream_id,
        admin,
        _source_label AS source_label,
        _institution_ror AS institution_ror,
        _extract_datetime AS extract_datetime,
        _load_datetime AS load_datetime,
        _institution_ror || '||' || _source_label || '||' || community_id::text AS community_bk
    FROM {{ ref('ldg_dspacedb5_community') }}
),
final AS (
    SELECT
        community_hk,
        community_bk,
        community_id,
        logo_bitstream_id,
        admin,
        source_label,
        institution_ror,
        extract_datetime,
        load_datetime
    FROM source s0
    CROSS JOIN LATERAL (
        SELECT {{ automate_dv.hash(columns='community_bk', alias='community_hk') }}
    ) s1
)

SELECT * FROM final
