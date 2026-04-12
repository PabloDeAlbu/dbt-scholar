{{ config(materialized='table') }}

WITH ir_handle AS (
    SELECT DISTINCT
        item_hk,
        item_id,
        NULLIF(BTRIM(handle_value), '') AS handle_normalized
    FROM {{ ref('fct_unlp_ir_item_publication') }}
    CROSS JOIN LATERAL REGEXP_SPLIT_TO_TABLE(COALESCE(handle, ''), '[|]') AS handle_value
    WHERE NULLIF(BTRIM(handle_value), '') IS NOT NULL
),

openaire_originalid_raw AS (
    SELECT DISTINCT
        publication.researchproduct_hk,
        publication.researchproduct_id,
        bridge.original_id,
        SUBSTRING(bridge.original_id FROM '(10915/[0-9]+)') AS handle_normalized
    FROM {{ ref('fct_unlp_openaire_researchproduct') }} AS publication
    INNER JOIN {{ ref('brg_openaire_researchproduct_originalid') }} AS bridge
        USING (researchproduct_hk)
    WHERE SUBSTRING(bridge.original_id FROM '(10915/[0-9]+)') IS NOT NULL
),

openaire_originalid AS (
    SELECT
        researchproduct_hk,
        researchproduct_id,
        MIN(original_id) AS original_id,
        handle_normalized
    FROM openaire_originalid_raw
    GROUP BY
        researchproduct_hk,
        researchproduct_id,
        handle_normalized
),

matched_pairs AS (
    SELECT DISTINCT
        ir_handle.item_hk,
        ir_handle.item_id,
        openaire_originalid.researchproduct_hk,
        openaire_originalid.researchproduct_id,
        openaire_originalid.original_id,
        ir_handle.handle_normalized
    FROM ir_handle
    INNER JOIN openaire_originalid USING (handle_normalized)
),

unlp_match_stats AS (
    SELECT
        item_hk,
        COUNT(DISTINCT researchproduct_hk) AS unlp_match_count
    FROM matched_pairs
    GROUP BY item_hk
),

openaire_match_stats AS (
    SELECT
        researchproduct_hk,
        COUNT(DISTINCT item_hk) AS openaire_match_count
    FROM matched_pairs
    GROUP BY researchproduct_hk
),

final AS (
    SELECT
        matched_pairs.item_hk,
        matched_pairs.item_id,
        matched_pairs.researchproduct_hk,
        matched_pairs.researchproduct_id,
        matched_pairs.original_id,
        matched_pairs.handle_normalized,
        COALESCE(unlp_match_stats.unlp_match_count, 0) AS unlp_match_count,
        COALESCE(openaire_match_stats.openaire_match_count, 0) AS openaire_match_count,
        (COALESCE(unlp_match_stats.unlp_match_count, 0) = 1) AS is_unique_unlp,
        (COALESCE(openaire_match_stats.openaire_match_count, 0) = 1) AS is_unique_openaire,
        (
            COALESCE(unlp_match_stats.unlp_match_count, 0) = 1
            AND COALESCE(openaire_match_stats.openaire_match_count, 0) = 1
        ) AS is_unique_match,
        'handle_from_original_id'::text AS match_rule
    FROM matched_pairs
    LEFT JOIN unlp_match_stats USING (item_hk)
    LEFT JOIN openaire_match_stats USING (researchproduct_hk)
)

SELECT * FROM final
