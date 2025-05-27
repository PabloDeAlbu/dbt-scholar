WITH base as (
    SELECT 
        hub_researchproduct.researchproduct_id,
        hub_researchproduct.researchproduct_hk,
        link_researchproduct_url.url_hk
    FROM {{ref('hub_openaire_researchproduct')}} hub_researchproduct
    INNER JOIN {{ref('link_openaire_researchproduct_url')}} link_researchproduct_url ON link_researchproduct_url.researchproduct_hk = hub_researchproduct.researchproduct_hk 
)

SELECT * FROM base