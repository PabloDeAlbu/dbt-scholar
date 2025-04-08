WITH base as (
    SELECT 
        hub_researchproduct.researchproduct_id,
        hub_researchproduct.researchproduct_hk,
        link_researchproduct_doi.doi_hk
    FROM {{ref('hub_openaire_researchproduct')}} hub_researchproduct
    INNER JOIN {{ref('link_openaire_researchproduct_doi')}} link_researchproduct_doi ON link_researchproduct_doi.researchproduct_hk = hub_researchproduct.researchproduct_hk 
)

SELECT * FROM base