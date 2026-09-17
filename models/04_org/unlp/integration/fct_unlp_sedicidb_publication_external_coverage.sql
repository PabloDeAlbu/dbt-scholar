{{ config(materialized='table') }}

WITH sedici_publication AS (
    SELECT item_id
    FROM {{ ref('fct_unlp_sedicidb_item_publication') }}
),

openalex_match AS (
    SELECT
        item_id,
        MAX(sedici_match_count)::int AS openalex_match_count,
        BOOL_OR(is_unique_match) AS has_unique_openalex_match,
        (ARRAY_AGG(work_hk) FILTER (WHERE is_unique_match))[1] AS work_hk,
        MIN(work_id) FILTER (WHERE is_unique_match) AS work_id
    FROM {{ ref('brg_unlp_sedicidb_openalex_publication') }}
    GROUP BY item_id
),

openaire_match AS (
    SELECT
        item_id,
        MAX(sedici_match_count)::int AS openaire_match_count,
        BOOL_OR(is_unique_match) AS has_unique_openaire_match,
        (ARRAY_AGG(researchproduct_hk) FILTER (WHERE is_unique_match))[1]
            AS researchproduct_hk,
        MIN(researchproduct_id) FILTER (WHERE is_unique_match) AS researchproduct_id
    FROM {{ ref('brg_unlp_sedicidb_openaire_publication') }}
    GROUP BY item_id
),

external_pair AS (
    SELECT
        work_hk,
        researchproduct_hk,
        BOOL_OR(is_unique_match) AS is_unique_match
    FROM {{ ref('brg_unlp_openalex_openaire_publication') }}
    GROUP BY
        work_hk,
        researchproduct_hk
),

coverage AS (
    SELECT
        sedici.item_id,
        openalex.item_id IS NOT NULL AS has_openalex_match,
        COALESCE(openalex.openalex_match_count, 0) AS openalex_match_count,
        COALESCE(openalex.has_unique_openalex_match, FALSE)
            AS has_unique_openalex_match,
        openalex.work_hk,
        openalex.work_id,
        openaire.item_id IS NOT NULL AS has_openaire_match,
        COALESCE(openaire.openaire_match_count, 0) AS openaire_match_count,
        COALESCE(openaire.has_unique_openaire_match, FALSE)
            AS has_unique_openaire_match,
        openaire.researchproduct_hk,
        openaire.researchproduct_id,
        CASE
            WHEN NOT COALESCE(openalex.has_unique_openalex_match, FALSE)
                OR NOT COALESCE(openaire.has_unique_openaire_match, FALSE)
                THEN 'not_applicable'
            WHEN pair.work_hk IS NULL
                THEN 'not_confirmed_by_doi'
            WHEN pair.is_unique_match
                THEN 'confirmed_unique'
            ELSE 'confirmed_ambiguous'
        END AS external_pair_status
    FROM sedici_publication AS sedici
    LEFT JOIN openalex_match AS openalex
        USING (item_id)
    LEFT JOIN openaire_match AS openaire
        USING (item_id)
    LEFT JOIN external_pair AS pair
        ON pair.work_hk = openalex.work_hk
       AND pair.researchproduct_hk = openaire.researchproduct_hk
)

SELECT *
FROM coverage
