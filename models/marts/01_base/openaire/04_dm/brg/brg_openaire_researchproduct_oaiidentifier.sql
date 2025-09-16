{{ config(materialized = 'table') }}

WITH base as (
    SELECT 
        hub_researchproduct.researchproduct_id,
        hub_researchproduct.researchproduct_hk,
        link_researchproduct_originalid.original_hk,
        hub_originalid.original_id
    FROM {{ref('hub_openaire_researchproduct')}} hub_researchproduct
    INNER JOIN {{ref('link_openaire_researchproduct_originalid')}} link_researchproduct_originalid ON link_researchproduct_originalid.researchproduct_hk = hub_researchproduct.researchproduct_hk
    INNER JOIN {{ref('hub_openaire_originalid')}} hub_originalid ON hub_originalid.original_hk = link_researchproduct_originalid.original_hk
    WHERE hub_originalid.original_id LIKE 'oai:%'
)

SELECT * FROM base