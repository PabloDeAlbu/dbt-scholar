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
        'oai'::text AS source_system,
        record_hk::text AS entity_hk_text,
        record_id::text AS entity_id,
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
        'openaire'::text AS source_system,
        publication.researchproduct_hk::text AS entity_hk_text,
        publication.researchproduct_id::text AS entity_id,
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

examples AS (
    SELECT * FROM oai_doi
    UNION ALL
    SELECT * FROM openaire_pid_doi
),

matched_doi AS (
    SELECT DISTINCT doi_normalized
    FROM {{ ref('brg_conicet_publication_doi') }}
),

final AS (
    SELECT
        examples.source_system,
        examples.entity_hk_text,
        examples.entity_id,
        examples.doi_raw,
        examples.doi_normalized,
        (matched_doi.doi_normalized IS NOT NULL) AS has_match
    FROM examples
    LEFT JOIN matched_doi
        USING (doi_normalized)
    WHERE examples.doi_raw IS NOT NULL
      AND TRIM(examples.doi_raw) <> ''
)

SELECT * FROM final
