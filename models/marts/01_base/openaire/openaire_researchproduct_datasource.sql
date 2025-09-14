with base AS (
    SELECT
        hub_rp.researchproduct_id,
        hub_ds.datasource_id,
        link_rp.researchproduct_hk,
        link_rp.datasource_hk,
        link_rp.researchproduct_datasource_hk
    FROM {{ref('hub_openaire_researchproduct')}} hub_rp
    INNER JOIN {{ref('link_openaire_researchproduct_datasource')}} link_rp USING (researchproduct_hk)
    INNER JOIN {{ref('hub_openaire_datasource')}} hub_ds USING (datasource_hk)
)

SELECT * FROM base
