WITH dedup_result AS (
    SELECT
        id_document1::text AS koha_id,
        id_document2,
        rules AS dedup_rules,
        similarity AS dedup_similarity
    FROM {{ ref('seed_issue_14365_03_dedup_thesis_koha_sedici_result') }}
),

automatic_candidate AS (
    SELECT
        result.koha_id,
        handle_match[1] AS sedici_handle,
        result.dedup_rules,
        result.dedup_similarity,
        COUNT(*) OVER (PARTITION BY result.koha_id) AS sedici_candidate_count
    FROM dedup_result AS result
    CROSS JOIN LATERAL REGEXP_MATCHES(
        result.id_document2,
        '(10915/[0-9]+)',
        'g'
    ) AS handle_match
    WHERE result.dedup_similarity = 'DUPLICATE'
),

automatic_candidate_count AS (
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY sedici_handle) AS koha_candidate_count
    FROM automatic_candidate
),

automatic_match AS (
    SELECT
        koha_id,
        sedici_handle,
        dedup_rules,
        dedup_similarity,
        'automatic'::text AS match_source,
        FALSE AS requires_metadata_review
    FROM automatic_candidate_count
    WHERE sedici_candidate_count = 1
      AND koha_candidate_count = 1
),

manual_match AS (
    SELECT
        review.koha_id::text,
        review.sedici_id AS sedici_handle,
        result.dedup_rules,
        result.dedup_similarity,
        'manual'::text AS match_source,
        TRUE AS requires_metadata_review
    FROM {{ ref('seed_issue_14365_03_review_thesis_koha_sedici_manual') }} AS review
    INNER JOIN dedup_result AS result
        ON result.koha_id = review.koha_id::text
    WHERE review.manual_decision = 'DUPLICATE'
),

confirmed_match AS (
    SELECT * FROM automatic_match
    UNION ALL
    SELECT * FROM manual_match
)

SELECT
    confirmed.koha_id,
    'https://artes.bibliotecas.unlp.edu.ar/cgi-bin/koha/opac-detail.pl?biblionumber='
        || confirmed.koha_id AS koha_url,
    publication.item_id AS sedici_item_id,
    confirmed.sedici_handle,
    'https://sedici.unlp.edu.ar/handle/' || confirmed.sedici_handle AS sedici_url,
    koha.title AS koha_title,
    koha.author AS koha_author,
    koha.date AS koha_date,
    sedici.title AS sedici_title,
    sedici.author AS sedici_author,
    sedici.date AS sedici_date,
    confirmed.match_source,
    confirmed.dedup_similarity,
    confirmed.dedup_rules,
    confirmed.requires_metadata_review
FROM confirmed_match AS confirmed
INNER JOIN {{ ref('unlp_sedici_03_dedup_thesis_koha_sedici_input_1') }} AS koha
    ON koha.id = confirmed.koha_id
INNER JOIN {{ ref('unlp_sedici_03_dedup_thesis_koha_sedici_input_2') }} AS sedici
    ON sedici.id = confirmed.sedici_handle
INNER JOIN {{ ref('fct_unlp_sedicidb_item_publication') }} AS publication
    ON publication.handle = confirmed.sedici_handle
