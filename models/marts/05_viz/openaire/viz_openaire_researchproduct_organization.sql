{{ config(materialized = 'view') }}

WITH base AS (
    SELECT 
        researchproduct_id,
        organization_id,
        acronym,
        legalname
    FROM {{ ref('hub_openaire_researchproduct') }}
    INNER JOIN {{ ref('brg_openaire_researchproduct_organization') }} brg USING (researchproduct_hk)
    INNER JOIN {{ ref('dim_openaire_organization') }} USING (organization_hk)
)

SELECT * FROM base
