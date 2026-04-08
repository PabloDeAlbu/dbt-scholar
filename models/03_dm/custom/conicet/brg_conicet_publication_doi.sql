{{ config(materialized = 'table') }}

WITH oai_base AS (
    SELECT
        record_hk,
        record_id,
        doi AS doi_raw
    FROM {{ ref('fct_conicet_publication') }}
    WHERE doi IS NOT NULL
      AND TRIM(doi) <> ''
),

oai_doi AS (
    SELECT
        record_hk,
        record_id,
        doi_raw,
        NULLIF(
            REGEXP_REPLACE(
                LOWER(TRIM(doi_raw)),
                '^(https?://(dx\\.)?doi\\.org/|doi:)',
                ''
            ),
            ''
        ) AS doi_normalized
    FROM oai_base
),

oai_doi_stats AS (
    SELECT
        record_hk,
        COUNT(DISTINCT doi_normalized) AS oai_doi_count
    FROM oai_doi
    WHERE doi_normalized IS NOT NULL
    GROUP BY record_hk
),

openaire_pid_doi AS (
    SELECT
        researchproduct_hk,
        value AS doi_raw,
        NULLIF(
            REGEXP_REPLACE(
                LOWER(TRIM(value)),
                '^(https?://(dx\\.)?doi\\.org/|doi:)',
                ''
            ),
            ''
        ) AS doi_normalized
    FROM {{ ref('brg_openaire_researchproduct_pid') }}
    WHERE LOWER(scheme) = 'doi'
      AND value IS NOT NULL
      AND TRIM(value) <> ''
),

openaire_doi AS (
    SELECT
        publication.researchproduct_hk,
        publication.researchproduct_id,
        pid.doi_raw,
        pid.doi_normalized
    FROM {{ ref('fct_conicet_openaire_researchproduct_publication') }} AS publication
    INNER JOIN openaire_pid_doi AS pid
        USING (researchproduct_hk)
),

openaire_doi_stats AS (
    SELECT
        researchproduct_hk,
        COUNT(DISTINCT doi_normalized) AS openaire_doi_count
    FROM openaire_doi
    WHERE doi_normalized IS NOT NULL
    GROUP BY researchproduct_hk
),

doi_match_counts AS (
    SELECT
        doi_normalized,
        COUNT(DISTINCT record_hk) AS oai_match_count,
        COUNT(DISTINCT researchproduct_hk) AS openaire_match_count
    FROM (
        SELECT
            doi_normalized,
            record_hk,
            NULL::bytea AS researchproduct_hk
        FROM oai_doi
        WHERE doi_normalized IS NOT NULL

        UNION ALL

        SELECT
            doi_normalized,
            NULL::bytea AS record_hk,
            researchproduct_hk
        FROM openaire_doi
        WHERE doi_normalized IS NOT NULL
    ) AS unioned
    GROUP BY doi_normalized
),

final AS (
    SELECT
        oai.record_hk,
        oai.record_id,
        openaire.researchproduct_hk,
        openaire.researchproduct_id,
        oai.doi_raw AS doi_raw_oai,
        openaire.doi_raw AS doi_raw_openaire,
        oai.doi_normalized,
        COALESCE(oai_stats.oai_doi_count, 0) AS oai_doi_count,
        COALESCE(openaire_stats.openaire_doi_count, 0) AS openaire_doi_count,
        COALESCE(match_counts.oai_match_count, 0) AS oai_match_count,
        COALESCE(match_counts.openaire_match_count, 0) AS openaire_match_count,
        (COALESCE(oai_stats.oai_doi_count, 0) = 1) AS is_unique_oai,
        (COALESCE(openaire_stats.openaire_doi_count, 0) = 1) AS is_unique_openaire,
        (
            COALESCE(match_counts.oai_match_count, 0) = 1
            AND COALESCE(match_counts.openaire_match_count, 0) = 1
        ) AS is_unique_match,
        'doi_exact'::text AS match_rule
    FROM oai_doi AS oai
    INNER JOIN openaire_doi AS openaire
        USING (doi_normalized)
    LEFT JOIN oai_doi_stats AS oai_stats
        USING (record_hk)
    LEFT JOIN openaire_doi_stats AS openaire_stats
        USING (researchproduct_hk)
    LEFT JOIN doi_match_counts AS match_counts
        USING (doi_normalized)
    WHERE oai.doi_normalized IS NOT NULL
)

SELECT * FROM final
