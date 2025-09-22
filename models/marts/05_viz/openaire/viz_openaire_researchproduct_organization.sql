WITH base AS (
    SELECT 
        hub.researchproduct_id,
        organization_id,
        acronym,
        legalname,
        publication_date
    FROM {{ ref('hub_openaire_researchproduct') }} hub
    INNER JOIN {{ ref('brg_openaire_researchproduct_organization') }} brg USING (researchproduct_hk)
    INNER JOIN {{ ref('dim_openaire_organization') }} USING (organization_hk)
    INNER JOIN {{ ref('fct_openaire_researchproduct_publication') }} USING (researchproduct_hk)
)

SELECT * FROM base
