WITH base as (
    SELECT 
        hub_researchproduct.researchproduct_id,
        hub_researchproduct.researchproduct_hk,
        link_researchproduct_arxiv.arxiv_hk
    FROM {{ref('hub_openaire_researchproduct')}} hub_researchproduct
    INNER JOIN {{ref('link_openaire_researchproduct_arxiv')}} link_researchproduct_arxiv ON link_researchproduct_arxiv.researchproduct_hk = hub_researchproduct.researchproduct_hk 
)

SELECT * FROM base