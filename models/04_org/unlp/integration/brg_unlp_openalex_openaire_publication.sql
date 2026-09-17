{{ config(materialized='table') }}

WITH openalex_doi AS (
    SELECT
        doi.work_hk,
        publication.work_id,
        doi.doi_hk,
        doi.doi_raw,
        doi.doi
    FROM {{ ref('brg_openalex_work_doi') }} AS doi
    INNER JOIN {{ ref('fct_openalex_work_publication') }} AS publication
        USING (work_hk)
),

openaire_doi AS (
    SELECT
        doi.researchproduct_hk,
        publication.researchproduct_id,
        doi.doi,
        doi.doi_raw,
        doi.observation_count
    FROM {{ ref('brg_openaire_researchproduct_doi') }} AS doi
    INNER JOIN {{ ref('fct_openaire_researchproduct_publication') }} AS publication
        USING (researchproduct_hk)
),

unlp_openalex_work AS (
    SELECT
        work_hk,
        unlp_first_extract_datetime,
        unlp_last_extract_datetime
    FROM {{ ref('fct_unlp_openalex_work_publication') }}
),

unlp_openaire_product AS (
    SELECT
        researchproduct_hk,
        unlp_first_extract_datetime,
        unlp_last_extract_datetime
    FROM {{ ref('fct_unlp_openaire_researchproduct') }}
),

matched_pair AS (
    SELECT DISTINCT
        openalex.work_hk,
        openalex.work_id,
        openalex.doi_hk,
        openalex.doi_raw AS openalex_doi_raw,
        openalex.doi,
        openaire.researchproduct_hk,
        openaire.researchproduct_id,
        openaire.doi_raw AS openaire_doi_raw,
        openaire.observation_count AS openaire_doi_observation_count,
        unlp_openalex.work_hk IS NOT NULL AS is_unlp_ror_work,
        unlp_openalex.unlp_first_extract_datetime
            AS openalex_unlp_first_extract_datetime,
        unlp_openalex.unlp_last_extract_datetime
            AS openalex_unlp_last_extract_datetime,
        unlp_openaire.researchproduct_hk IS NOT NULL AS is_unlp_ror_product,
        unlp_openaire.unlp_first_extract_datetime
            AS openaire_unlp_first_extract_datetime,
        unlp_openaire.unlp_last_extract_datetime
            AS openaire_unlp_last_extract_datetime
    FROM openalex_doi AS openalex
    INNER JOIN openaire_doi AS openaire
        USING (doi)
    LEFT JOIN unlp_openalex_work AS unlp_openalex
        USING (work_hk)
    LEFT JOIN unlp_openaire_product AS unlp_openaire
        USING (researchproduct_hk)
),

openalex_match_count AS (
    SELECT
        work_hk,
        COUNT(DISTINCT researchproduct_hk) AS openalex_match_count
    FROM matched_pair
    GROUP BY work_hk
),

openaire_match_count AS (
    SELECT
        researchproduct_hk,
        COUNT(DISTINCT work_hk) AS openaire_match_count
    FROM matched_pair
    GROUP BY researchproduct_hk
)

SELECT
    matched_pair.work_hk,
    matched_pair.work_id,
    matched_pair.doi_hk,
    matched_pair.openalex_doi_raw,
    matched_pair.doi,
    matched_pair.researchproduct_hk,
    matched_pair.researchproduct_id,
    matched_pair.openaire_doi_raw,
    matched_pair.openaire_doi_observation_count,
    openalex_match_count.openalex_match_count,
    openaire_match_count.openaire_match_count,
    openalex_match_count.openalex_match_count = 1
        AND openaire_match_count.openaire_match_count = 1 AS is_unique_match,
    matched_pair.is_unlp_ror_work,
    matched_pair.openalex_unlp_first_extract_datetime,
    matched_pair.openalex_unlp_last_extract_datetime,
    matched_pair.is_unlp_ror_product,
    matched_pair.openaire_unlp_first_extract_datetime,
    matched_pair.openaire_unlp_last_extract_datetime,
    'doi_exact'::text AS match_rule
FROM matched_pair
INNER JOIN openalex_match_count
    USING (work_hk)
INNER JOIN openaire_match_count
    USING (researchproduct_hk)
