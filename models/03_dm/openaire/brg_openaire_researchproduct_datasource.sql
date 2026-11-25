-- ¿Qué relaciones entre researchproducts y datasources conserva OpenAIRE?
WITH relationship AS (
    SELECT 
        link_ds.researchproduct_hk,
        link_ds.datasource_hk
    FROM {{ref('link_openaire_researchproduct_datasource')}} link_ds
),

-- ¿Qué datasources participaron en la recolección de cada researchproduct?
final AS (
    SELECT
        researchproduct_hk,
        datasource_hk
    FROM relationship
)

SELECT *
FROM final
