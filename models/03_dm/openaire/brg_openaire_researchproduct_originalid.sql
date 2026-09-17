{{ config(materialized = 'table') }}

WITH base AS (
    SELECT
        link.researchproduct_hk,
        link.original_hk,
        link.researchproduct_originalid_hk,
        original.original_id
    FROM {{ ref('link_openaire_researchproduct_originalid') }} AS link
    INNER JOIN {{ ref('hub_openaire_originalid') }} AS original
        USING (original_hk)
)

SELECT * FROM base
