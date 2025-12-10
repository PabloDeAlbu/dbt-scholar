WITH base AS (
    SELECT 
        link_ds.researchproduct_hk,
        link_ds.datasource_hk
    FROM {{ref('link_openaire_researchproduct_datasource')}} link_ds
)

SELECT * FROM base