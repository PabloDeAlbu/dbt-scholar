WITH base as (
    SELECT 
        hub_researchproduct.researchproduct_id,
        hub_researchproduct.researchproduct_hk,
        link_researchproduct_mag.mag_hk
    FROM {{ref('hub_openaire_researchproduct')}} hub_researchproduct
    INNER JOIN {{ref('link_openaire_researchproduct_mag')}} link_researchproduct_mag ON link_researchproduct_mag.researchproduct_hk = hub_researchproduct.researchproduct_hk 
)

SELECT * FROM base