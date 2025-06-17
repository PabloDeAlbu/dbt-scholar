WITH base as (
    SELECT 
        hub_doi.doi,
        hub_doi.doi_hk
    FROM {{ref('brg_openaire_researchproduct_doi')}} bridge
    INNER JOIN {{ref('hub_openaire_doi')}} hub_doi ON bridge.doi_hk = hub_doi.doi_hk
)

SELECT * FROM base