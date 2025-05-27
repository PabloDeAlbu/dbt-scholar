WITH base as (
    SELECT 
        hub_researchproduct.researchproduct_id,
        hub_researchproduct.researchproduct_hk,
        link_researchproduct_pmc.pmc_hk
    FROM {{ref('hub_openaire_researchproduct')}} hub_researchproduct
    INNER JOIN {{ref('link_openaire_researchproduct_pmc')}} link_researchproduct_pmc ON link_researchproduct_pmc.researchproduct_hk = hub_researchproduct.researchproduct_hk 
)

SELECT * FROM base