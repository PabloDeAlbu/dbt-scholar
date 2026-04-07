{{ config(materialized='view') }}

WITH source AS (
    SELECT
        community_community_id,
        parent_comm_id,
        child_comm_id,
        _source_label AS source_label,
        _institution_ror AS institution_ror,
        _extract_datetime AS extract_datetime,
        _load_datetime AS load_datetime,
        _institution_ror || '||' || _source_label || '||' || parent_comm_id::text AS parent_comm_bk,
        _institution_ror || '||' || _source_label || '||' || child_comm_id::text AS child_comm_bk,
        _institution_ror || '||' || _source_label || '||' || community_community_id::text AS community_community_bk
    FROM {{ ref('ldg_dspacedb5_community2community') }}
),
final AS (
    SELECT
        parent_comm_hk,
        child_comm_hk,
        community_community_hk,
        parent_comm_bk,
        child_comm_bk,
        community_community_bk,
        community_community_id,
        parent_comm_id,
        child_comm_id,
        source_label,
        institution_ror,
        extract_datetime,
        load_datetime
    FROM source s0
    CROSS JOIN LATERAL (
        SELECT
            {{ automate_dv.hash(columns='parent_comm_bk', alias='parent_comm_hk') }},
            {{ automate_dv.hash(columns='child_comm_bk', alias='child_comm_hk') }},
            {{ automate_dv.hash(columns='community_community_bk', alias='community_community_hk') }}
    ) s1
)

SELECT * FROM final
