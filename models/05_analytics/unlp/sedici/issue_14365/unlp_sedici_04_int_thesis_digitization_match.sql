WITH duplicate_result AS (
    SELECT
        id_document1::text AS koha_id,
        id_document2
    FROM {{ ref('seed_issue_14365_04_dedup_thesis_digitization_result') }}
    WHERE similarity = 'DUPLICATE'
),

candidate AS (
    SELECT
        koha_id,
        inventory_match[1] AS inventory_id,
        COUNT(*) OVER (PARTITION BY koha_id) AS inventory_candidate_count
    FROM duplicate_result
    CROSS JOIN LATERAL REGEXP_MATCHES(
        id_document2,
        '([0-9]+)',
        'g'
    ) AS inventory_match
),

candidate_count AS (
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY inventory_id) AS koha_candidate_count
    FROM candidate
),

automatic_match AS (
    SELECT
        koha_id,
        inventory_id,
        koha_id AS canonical_koha_id,
        'DUPLICATE'::text AS match_status,
        'automatic'::text AS match_source
    FROM candidate_count
    WHERE inventory_candidate_count = 1
      AND koha_candidate_count = 1
),

manual_match AS (
    SELECT
        koha_id::text,
        inventory_id::text,
        canonical_koha_id::text,
        manual_decision AS match_status,
        'manual'::text AS match_source
    FROM {{ ref('seed_issue_14365_04_review_thesis_digitization_manual') }}
)

SELECT * FROM automatic_match
UNION ALL
SELECT * FROM manual_match
