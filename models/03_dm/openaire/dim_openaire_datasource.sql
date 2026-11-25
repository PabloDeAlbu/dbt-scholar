-- ¿Qué atributos describen cada datasource registrado por OpenAIRE?
WITH datasource AS (
    SELECT 
        hub_ds.datasource_id,
        sat_ds.value AS datasource_name,
        hub_ds.datasource_hk
    FROM {{ ref('hub_openaire_datasource') }} hub_ds
    JOIN {{ latest_satellite(ref('sat_openaire_datasource'), 'datasource_hk') }} AS sat_ds USING (datasource_hk)
),

-- ¿Qué datasources de OpenAIRE quedan disponibles para relacionarlos con researchproducts?
final AS (
    SELECT
        datasource_id,
        datasource_name,
        datasource_hk
    FROM datasource
)

SELECT *
FROM final
