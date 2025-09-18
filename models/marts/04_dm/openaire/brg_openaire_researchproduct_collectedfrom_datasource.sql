WITH base AS (
    SELECT 
        hub_ds.datasource_id,
        sat_ds.value,
        hub_ds.datasource_hk
    FROM {{ref('hub_openaire_datasource')}} hub_ds
    INNER JOIN {{ref('sat_openaire_datasource')}} sat_ds USING (datasource_hk)
)

SELECT * FROM base