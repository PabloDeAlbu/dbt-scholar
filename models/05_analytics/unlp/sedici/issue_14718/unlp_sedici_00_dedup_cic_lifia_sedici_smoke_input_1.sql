WITH selected AS (
    SELECT candidate.cic_id
    FROM {{ ref('unlp_sedici_00_dedup_cic_lifia_sedici_smoke_candidate') }} AS candidate
    GROUP BY candidate.cic_id
    ORDER BY MIN(candidate.signal_priority), MD5(candidate.cic_id)
    LIMIT 100
)

SELECT input.*
FROM {{ ref('unlp_sedici_01_dedup_cic_lifia_sedici_input_1') }} AS input
INNER JOIN selected
    ON selected.cic_id = input.id
