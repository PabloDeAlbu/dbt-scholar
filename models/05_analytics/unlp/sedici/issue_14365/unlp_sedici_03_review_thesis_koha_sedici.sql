WITH dedup_result AS (
    SELECT
        id_document1::text AS koha_id,
        NULLIF(BTRIM(id_document2), 'None') AS sedici_candidates,
        NULLIF(BTRIM(rules), 'None') AS dedup_rules,
        similarity AS dedup_similarity
    FROM {{ ref('seed_issue_14365_03_dedup_thesis_koha_sedici_result') }}
),

candidate_url AS (
    SELECT
        result.koha_id,
        STRING_AGG(
            'https://sedici.unlp.edu.ar/handle/' || handle_match[1],
            ' | '
            ORDER BY handle_match[1]
        ) AS sedici_urls
    FROM dedup_result AS result
    CROSS JOIN LATERAL REGEXP_MATCHES(
        result.sedici_candidates,
        '(10915/[0-9]+)',
        'g'
    ) AS handle_match
    GROUP BY result.koha_id
)

SELECT
    koha.id AS koha_id,
    'https://artes.bibliotecas.unlp.edu.ar/cgi-bin/koha/opac-detail.pl?biblionumber='
        || koha.id AS koha_url,
    koha.title,
    koha.author,
    koha.date,
    result.sedici_candidates,
    candidate_url.sedici_urls,
    result.dedup_rules,
    result.dedup_similarity
FROM {{ ref('unlp_sedici_03_dedup_thesis_koha_sedici_input_1') }} AS koha
INNER JOIN dedup_result AS result
    ON result.koha_id = koha.id
INNER JOIN candidate_url
    ON candidate_url.koha_id = koha.id
WHERE result.dedup_similarity IN ('NEAR_DUPLICATE', 'DONT_KNOW')
