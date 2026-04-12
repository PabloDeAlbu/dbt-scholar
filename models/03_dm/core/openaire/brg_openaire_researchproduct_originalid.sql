{{ config(materialized = 'table') }}

WITH base AS (
    SELECT
        link.researchproduct_hk,
        link.original_hk,
        link.researchproduct_originalid_hk,
        stg.original_id
    FROM {{ ref('link_openaire_researchproduct_originalid') }} AS link
    INNER JOIN {{ ref('stg_openaire_researchproduct_originalid') }} AS stg
        USING (researchproduct_originalid_hk)
)

SELECT * FROM base
