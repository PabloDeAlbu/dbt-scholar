WITH selected_cic AS (
    SELECT id AS cic_id
    FROM {{ ref('unlp_sedici_00_dedup_cic_lifia_sedici_smoke_input_1') }}
),

selected_sedici AS (
    SELECT candidate.sedici_id
    FROM {{ ref('unlp_sedici_00_dedup_cic_lifia_sedici_smoke_candidate') }} AS candidate
    INNER JOIN selected_cic
        USING (cic_id)
    GROUP BY candidate.sedici_id
    ORDER BY MIN(candidate.signal_priority), MD5(candidate.sedici_id)
    LIMIT 100
)

SELECT input.*
FROM {{ ref('unlp_sedici_01_dedup_cic_lifia_sedici_input_2') }} AS input
INNER JOIN selected_sedici
    ON selected_sedici.sedici_id = input.id
