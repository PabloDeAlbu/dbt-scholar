{{ config(materialized='table') }}

WITH ir_base AS (
    SELECT
        item_hk,
        item_id,
        doi
    FROM {{ ref('fct_unlp_ir_item_publication') }}
    WHERE discoverable = TRUE
      AND in_archive = TRUE
      AND withdrawn = FALSE
      AND doi IS NOT NULL
      AND TRIM(doi) <> ''
),

ir_doi AS (
    SELECT DISTINCT
        item_hk,
        item_id,
        NULLIF(BTRIM(pid_value), '') AS doi_normalized
    FROM ir_base
    CROSS JOIN LATERAL REGEXP_SPLIT_TO_TABLE(COALESCE(doi, ''), '[|]') AS pid_value
    WHERE NULLIF(BTRIM(pid_value), '') IS NOT NULL
),

openalex_doi AS (
    SELECT
        work_hk,
        work_id,
        doi AS doi_normalized
    FROM {{ ref('fct_unlp_openalex_work_publication') }}
    WHERE doi IS NOT NULL
      AND TRIM(doi) <> ''
),

matched_pairs AS (
    SELECT DISTINCT
        ir_doi.item_hk,
        ir_doi.item_id,
        openalex_doi.work_hk,
        openalex_doi.work_id,
        ir_doi.doi_normalized
    FROM ir_doi
    INNER JOIN openalex_doi USING (doi_normalized)
),

unlp_match_stats AS (
    SELECT
        item_hk,
        COUNT(DISTINCT work_hk) AS unlp_match_count
    FROM matched_pairs
    GROUP BY item_hk
),

openalex_match_stats AS (
    SELECT
        work_hk,
        COUNT(DISTINCT item_hk) AS openalex_match_count
    FROM matched_pairs
    GROUP BY work_hk
),

final AS (
    SELECT
        matched_pairs.item_hk,
        matched_pairs.item_id,
        matched_pairs.work_hk,
        matched_pairs.work_id,
        matched_pairs.doi_normalized,
        COALESCE(unlp_match_stats.unlp_match_count, 0) AS unlp_match_count,
        COALESCE(openalex_match_stats.openalex_match_count, 0) AS openalex_match_count,
        (COALESCE(unlp_match_stats.unlp_match_count, 0) = 1) AS is_unique_unlp,
        (COALESCE(openalex_match_stats.openalex_match_count, 0) = 1) AS is_unique_openalex,
        (
            COALESCE(unlp_match_stats.unlp_match_count, 0) = 1
            AND COALESCE(openalex_match_stats.openalex_match_count, 0) = 1
        ) AS is_unique_match,
        'doi_exact'::text AS match_rule
    FROM matched_pairs
    LEFT JOIN unlp_match_stats USING (item_hk)
    LEFT JOIN openalex_match_stats USING (work_hk)
)

SELECT * FROM final
