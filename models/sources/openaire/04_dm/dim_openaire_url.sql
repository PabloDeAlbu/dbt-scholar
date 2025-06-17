WITH base as (
    SELECT 
        hub_url.url,
        hub_url.url_hk
    FROM {{ref('brg_openaire_researchproduct_url')}} bridge
    INNER JOIN {{ref('hub_openaire_url')}} hub_url ON bridge.url_hk = hub_url.url_hk
)

SELECT * FROM base