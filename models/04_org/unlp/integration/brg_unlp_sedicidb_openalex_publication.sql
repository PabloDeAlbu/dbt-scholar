{{ config(materialized='table') }}

WITH sedici_doi AS (
    SELECT
        item_id,
        doi,
        metadatafields,
        observation_count
    FROM {{ ref('brg_unlp_sedicidb_item_doi') }}
),

openalex_doi AS (
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

unlp_openalex_work AS (
    SELECT
        work_hk,
        unlp_first_extract_datetime,
        unlp_last_extract_datetime
    FROM {{ ref('fct_unlp_openalex_work_publication') }}
),

matched_pair AS (
    SELECT DISTINCT
        sedici.item_id,
        sedici.doi,
        sedici.metadatafields AS sedici_metadatafields,
        sedici.observation_count AS sedici_doi_observation_count,
        openalex.work_hk,
        openalex.work_id,
        openalex.doi_hk,
        openalex.doi_raw AS openalex_doi_raw,
        unlp_openalex.work_hk IS NOT NULL AS is_unlp_ror_work,
        unlp_openalex.unlp_first_extract_datetime,
        unlp_openalex.unlp_last_extract_datetime
    FROM sedici_doi AS sedici
    INNER JOIN openalex_doi AS openalex
        USING (doi)
    LEFT JOIN unlp_openalex_work AS unlp_openalex
        USING (work_hk)
),

sedici_match_count AS (
    SELECT
        item_id,
        COUNT(DISTINCT work_hk) AS sedici_match_count
    FROM matched_pair
    GROUP BY item_id
),

openalex_match_count AS (
    SELECT
        work_hk,
        COUNT(DISTINCT item_id) AS openalex_match_count
    FROM matched_pair
    GROUP BY work_hk
)

SELECT
    matched_pair.item_id,
    matched_pair.doi,
    matched_pair.sedici_metadatafields,
    matched_pair.sedici_doi_observation_count,
    matched_pair.work_hk,
    matched_pair.work_id,
    matched_pair.doi_hk,
    matched_pair.openalex_doi_raw,
    sedici_match_count.sedici_match_count,
    openalex_match_count.openalex_match_count,
    sedici_match_count.sedici_match_count = 1
        AND openalex_match_count.openalex_match_count = 1 AS is_unique_match,
    matched_pair.is_unlp_ror_work,
    matched_pair.unlp_first_extract_datetime,
    matched_pair.unlp_last_extract_datetime,
    'doi_exact'::text AS match_rule
FROM matched_pair
INNER JOIN sedici_match_count
    USING (item_id)
INNER JOIN openalex_match_count
    USING (work_hk)
