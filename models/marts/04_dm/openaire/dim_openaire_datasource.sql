WITH base AS (
    SELECT 
        hub_ds.datasource_id,
        sat_ds.value AS datasource_name,
        hub_ds.datasource_hk,
        sat_ds.load_datetime
    FROM {{ ref('hub_openaire_datasource') }} hub_ds
    JOIN {{ ref('sat_openaire_datasource') }} sat_ds USING (datasource_hk)
),
picked AS (
    SELECT DISTINCT ON (datasource_hk)
        datasource_id,
        datasource_name,
        datasource_hk
    FROM base
    ORDER BY
        datasource_hk,
        load_datetime DESC,                -- 1) el más reciente
        (datasource_name IS NULL),         -- 2) preferí no nulos
        length(trim(datasource_name)) DESC -- 3) si empata, el más “largo”
)

SELECT * FROM picked
