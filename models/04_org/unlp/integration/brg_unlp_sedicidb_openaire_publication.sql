{{ config(materialized='table') }}

WITH sedici_publication AS (
    SELECT
        item_id,
        handle
    FROM {{ ref('fct_unlp_sedicidb_item_publication') }}
    WHERE handle IS NOT NULL
),

openaire_original_id_raw AS (
    SELECT DISTINCT
        publication.researchproduct_hk,
        publication.researchproduct_id,
        original_id.original_id,
        SUBSTRING(original_id.original_id FROM '(10915/[0-9]+)$') AS handle
    FROM {{ ref('brg_openaire_researchproduct_originalid') }} AS original_id
    INNER JOIN {{ ref('fct_openaire_researchproduct_publication') }} AS publication
        USING (researchproduct_hk)
    WHERE SUBSTRING(original_id.original_id FROM '(10915/[0-9]+)$') IS NOT NULL
),

openaire_original_id AS (
    SELECT
        researchproduct_hk,
        researchproduct_id,
        handle,
        MIN(original_id) AS original_id
    FROM openaire_original_id_raw
    GROUP BY
        researchproduct_hk,
        researchproduct_id,
        handle
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
        sedici.item_id,
        sedici.handle,
        openaire.researchproduct_hk,
        openaire.researchproduct_id,
        openaire.original_id,
        unlp_openaire.researchproduct_hk IS NOT NULL AS is_unlp_ror_product,
        unlp_openaire.unlp_first_extract_datetime,
        unlp_openaire.unlp_last_extract_datetime
    FROM sedici_publication AS sedici
    INNER JOIN openaire_original_id AS openaire
        USING (handle)
    LEFT JOIN unlp_openaire_product AS unlp_openaire
        USING (researchproduct_hk)
),

sedici_match_count AS (
    SELECT
        item_id,
        COUNT(DISTINCT researchproduct_hk) AS sedici_match_count
    FROM matched_pair
    GROUP BY item_id
),

openaire_match_count AS (
    SELECT
        researchproduct_hk,
        COUNT(DISTINCT item_id) AS openaire_match_count
    FROM matched_pair
    GROUP BY researchproduct_hk
)

SELECT
    matched_pair.item_id,
    matched_pair.handle,
    matched_pair.researchproduct_hk,
    matched_pair.researchproduct_id,
    matched_pair.original_id,
    sedici_match_count.sedici_match_count,
    openaire_match_count.openaire_match_count,
    sedici_match_count.sedici_match_count = 1
        AND openaire_match_count.openaire_match_count = 1 AS is_unique_match,
    matched_pair.is_unlp_ror_product,
    matched_pair.unlp_first_extract_datetime,
    matched_pair.unlp_last_extract_datetime,
    'handle_from_original_id'::text AS match_rule
FROM matched_pair
INNER JOIN sedici_match_count
    USING (item_id)
INNER JOIN openaire_match_count
    USING (researchproduct_hk)
