WITH automatic_no_sedici_match AS (
    SELECT id_document1::text AS koha_id
    FROM {{ ref('seed_issue_14365_03_dedup_thesis_koha_sedici_result') }}
    WHERE similarity = 'NO_DUPLICATE'
),

manual_no_sedici_match AS (
    SELECT koha_id::text
    FROM {{ ref('seed_issue_14365_03_review_thesis_koha_sedici_manual') }}
    WHERE manual_decision = 'NO_DUPLICATE'
),

no_sedici_match AS (
    SELECT koha_id FROM automatic_no_sedici_match
    UNION
    SELECT koha_id FROM manual_no_sedici_match
)

SELECT
    koha.source,
    koha.id,
    koha.title,
    koha.subtitle,
    koha.type,
    koha.author,
    koha.date,
    koha.doi,
    koha.isbn,
    koha.issn,
    koha.description
FROM {{ ref('unlp_sedici_03_dedup_thesis_koha_sedici_input_1') }} AS koha
INNER JOIN no_sedici_match
    ON no_sedici_match.koha_id = koha.id
