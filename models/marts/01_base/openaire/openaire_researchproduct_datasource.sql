with base AS (
    SELECT
        hub_ds.datasource_id,
        sat_ds.value as datasource_name,
        link_rp.researchproduct_hk,
        link_rp.datasource_hk,
        link_rp.researchproduct_datasource_hk
    FROM {{ref('hub_openaire_datasource')}} hub_ds
    INNER JOIN {{ref('link_openaire_researchproduct_datasource')}} link_rp USING (datasource_hk)
    INNER JOIN {{ref('sat_openaire_datasource')}} sat_ds USING (datasource_hk)
)

SELECT * FROM base
