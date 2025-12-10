WITH base AS (
    SELECT 
        hub_ds.datasource_id,
        sat_ds.value AS datasource_name,
        hub_ds.datasource_hk
    FROM {{ ref('hub_openaire_datasource') }} hub_ds
    JOIN {{ latest_satellite(ref('sat_openaire_datasource'), 'datasource_hk') }} AS sat_ds USING (datasource_hk)
)

SELECT * FROM base
