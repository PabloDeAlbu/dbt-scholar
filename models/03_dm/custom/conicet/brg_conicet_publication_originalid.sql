{{ config(materialized = 'table') }}

WITH oai_base AS (
    SELECT
        record_hk,
        record_id
    FROM {{ ref('fct_conicet_oai_record_publication') }}
),

openaire_originalid AS (
    SELECT
        publication.researchproduct_hk,
        publication.researchproduct_id,
        bridge.original_id
    FROM {{ ref('fct_conicet_openaire_researchproduct_publication') }} AS publication
    INNER JOIN {{ ref('brg_openaire_researchproduct_originalid') }} AS bridge
        USING (researchproduct_hk)
    WHERE bridge.original_id LIKE 'oai:ri.conicet.gov.ar:%'
),

originalid_match_counts AS (
    SELECT
        original_id,
        COUNT(DISTINCT researchproduct_hk) AS openaire_match_count
    FROM openaire_originalid
    GROUP BY original_id
),

final AS (
    SELECT
        oai.record_hk,
        oai.record_id,
        openaire.researchproduct_hk,
        openaire.researchproduct_id,
        openaire.original_id,
        COALESCE(match_counts.openaire_match_count, 0) AS openaire_match_count,
        (COALESCE(match_counts.openaire_match_count, 0) = 1) AS is_unique_match,
        'original_id_exact'::text AS match_rule
    FROM oai_base AS oai
    INNER JOIN openaire_originalid AS openaire
        ON openaire.original_id = oai.record_id
    LEFT JOIN originalid_match_counts AS match_counts
        ON match_counts.original_id = openaire.original_id
)

SELECT * FROM final
