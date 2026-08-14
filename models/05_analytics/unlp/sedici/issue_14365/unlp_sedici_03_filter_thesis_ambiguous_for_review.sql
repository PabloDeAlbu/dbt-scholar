WITH duplicate_result AS (
    SELECT
        id_document1::text AS koha_id,
        id_document2,
        rules
    FROM {{ ref('seed_issue_14365_03_dedup_thesis_koha_sedici_result') }}
    WHERE similarity = 'DUPLICATE'
),

candidate AS (
    SELECT
        koha_id,
        handle_match[1] AS sedici_id,
        NULLIF(BTRIM(rules), 'None') AS dedup_rules,
        COUNT(*) OVER (PARTITION BY koha_id) AS sedici_candidate_count
    FROM duplicate_result
    CROSS JOIN LATERAL REGEXP_MATCHES(
        id_document2,
        '(10915/[0-9]+)',
        'g'
    ) AS handle_match
),

candidate_count AS (
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY sedici_id) AS koha_candidate_count
    FROM candidate
)

SELECT
    koha_id,
    sedici_id,
    sedici_candidate_count,
    koha_candidate_count,
    dedup_rules
FROM candidate_count
WHERE sedici_candidate_count > 1
   OR koha_candidate_count > 1
