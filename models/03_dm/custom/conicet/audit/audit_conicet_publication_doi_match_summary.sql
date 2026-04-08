{{ config(materialized='table') }}

WITH base AS (
    SELECT *
    FROM {{ ref('brg_conicet_publication_doi') }}
),

final AS (
    SELECT
        match_rule,
        COUNT(*) AS match_row_count,
        COUNT(DISTINCT record_hk) AS matched_oai_entity_count,
        COUNT(DISTINCT researchproduct_hk) AS matched_openaire_entity_count,
        COUNT(DISTINCT doi_normalized) AS matched_distinct_doi_count,
        COUNT(*) FILTER (WHERE is_unique_oai) AS unique_oai_row_count,
        COUNT(*) FILTER (WHERE is_unique_openaire) AS unique_openaire_row_count,
        COUNT(*) FILTER (WHERE is_unique_match) AS unique_match_row_count,
        COUNT(*) FILTER (WHERE NOT is_unique_match) AS ambiguous_match_row_count
    FROM base
    GROUP BY match_rule
)

SELECT * FROM final
