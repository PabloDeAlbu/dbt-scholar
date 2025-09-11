{# WITH openalex AS (
    SELECT 
        fct.doi,
        brg.work_author_hk,
        brg.work_hk,
        brg.author_hk
    FROM {{ref('brg_openalex_publication_author')}} brg
    INNER JOIN {{ref('fct_openalex_work_publication')}} fct ON 
        fct.work_hk = brg.work_hk
), 

openaire AS (
    SELECT
        fct.doi,
        brg.researchproduct_orcid_hk,
        brg.researchproduct_hk,
        brg.orcid_hk
    FROM {{ref('brg_openaire_unlp_publication_orcid')}} brg
    INNER JOIN {{ref('fct_openaire_researchproduct')}} fct ON 
        fct.researchproduct_hk = brg.researchproduct_hk
)

SELECT * FROM openalex  #}
