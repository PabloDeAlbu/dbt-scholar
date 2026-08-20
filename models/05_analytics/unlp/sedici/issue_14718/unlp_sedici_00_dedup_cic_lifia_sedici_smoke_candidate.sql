WITH cic AS (
    SELECT *
    FROM {{ ref('unlp_sedici_01_dedup_cic_lifia_sedici_input_1') }}
),

sedici AS (
    SELECT *
    FROM {{ ref('unlp_sedici_01_dedup_cic_lifia_sedici_input_2') }}
),

doi_match AS (
    SELECT DISTINCT
        cic.id AS cic_id,
        sedici.id AS sedici_id,
        'exact_doi'::text AS candidate_signal,
        1 AS signal_priority
    FROM cic
    CROSS JOIN LATERAL UNNEST(STRING_TO_ARRAY(cic.doi, '|')) AS cic_doi(doi)
    INNER JOIN sedici
        ON cic_doi.doi = ANY(STRING_TO_ARRAY(sedici.doi, '|'))
    WHERE NULLIF(cic_doi.doi, '') IS NOT NULL
),

title_match AS (
    SELECT
        cic.id AS cic_id,
        sedici.id AS sedici_id,
        'exact_title'::text AS candidate_signal,
        2 AS signal_priority
    FROM cic
    INNER JOIN sedici
        ON LOWER(REGEXP_REPLACE(cic.title, '[^[:alnum:]]', '', 'g'))
         = LOWER(REGEXP_REPLACE(sedici.title, '[^[:alnum:]]', '', 'g'))
),

candidate AS (
    SELECT * FROM doi_match
    UNION ALL
    SELECT * FROM title_match
)

SELECT
    cic_id,
    sedici_id,
    STRING_AGG(DISTINCT candidate_signal, '|' ORDER BY candidate_signal) AS candidate_signal,
    MIN(signal_priority) AS signal_priority
FROM candidate
GROUP BY cic_id, sedici_id
