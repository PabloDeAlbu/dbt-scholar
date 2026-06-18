{{ config(materialized='table') }}

WITH oai_base AS (
    SELECT
        record_hk,
        record_id,
        doi AS doi_raw
    FROM {{ ref('fct_conicet_publication') }}
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

openaire_pid_doi AS (
    SELECT
        publication.researchproduct_hk,
        publication.researchproduct_id,
        pid.value AS doi_raw,
        NULLIF(
            REGEXP_REPLACE(
                LOWER(TRIM(pid.value)),
                '^(https?://(dx\\.)?doi\\.org/|doi:)',
                ''
            ),
            ''
        ) AS doi_normalized
    FROM {{ ref('fct_conicet_openaire_researchproduct_publication') }} AS publication
    INNER JOIN {{ ref('brg_openaire_researchproduct_pid') }} AS pid
        USING (researchproduct_hk)
    WHERE LOWER(pid.scheme) = 'doi'
),

source_summary AS (
    SELECT
        'oai'::text AS source_system,
        COUNT(DISTINCT record_hk) AS entity_count,
        COUNT(DISTINCT record_hk) FILTER (WHERE doi_raw IS NOT NULL AND TRIM(doi_raw) <> '') AS entity_with_doi_raw_count,
        COUNT(DISTINCT record_hk) FILTER (WHERE doi_normalized IS NOT NULL) AS entity_with_doi_normalized_count,
        COUNT(DISTINCT doi_normalized) AS distinct_doi_normalized_count
    FROM oai_doi

    UNION ALL

    SELECT
        'openaire'::text AS source_system,
        COUNT(DISTINCT researchproduct_hk) AS entity_count,
        COUNT(DISTINCT researchproduct_hk) FILTER (WHERE doi_raw IS NOT NULL AND TRIM(doi_raw) <> '') AS entity_with_doi_raw_count,
        COUNT(DISTINCT researchproduct_hk) FILTER (WHERE doi_normalized IS NOT NULL) AS entity_with_doi_normalized_count,
        COUNT(DISTINCT doi_normalized) AS distinct_doi_normalized_count
    FROM openaire_pid_doi
),

match_summary AS (
    SELECT
        COUNT(*) AS match_row_count,
        COUNT(*) FILTER (WHERE is_unique_match) AS unique_match_row_count,
        COUNT(DISTINCT doi_normalized) AS matched_distinct_doi_count
    FROM {{ ref('brg_conicet_publication_doi') }}
),

final AS (
    SELECT
        source_summary.source_system,
        source_summary.entity_count,
        source_summary.entity_with_doi_raw_count,
        source_summary.entity_with_doi_normalized_count,
        source_summary.distinct_doi_normalized_count,
        match_summary.match_row_count,
        match_summary.unique_match_row_count,
        match_summary.matched_distinct_doi_count
    FROM source_summary
    CROSS JOIN match_summary
)

SELECT * FROM final
