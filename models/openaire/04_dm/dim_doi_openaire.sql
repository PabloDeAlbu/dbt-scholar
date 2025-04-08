WITH base as (
    SELECT 
        hub_doi.doi,
        hub_doi.doi_hk
    FROM {{ref('bridge_publication_doi_openaire')}} bridge
    INNER JOIN {{ref('hub_openaire_doi')}} hub_doi ON bridge.doi_hk = hub_doi.doi_hk
)

SELECT * FROM base