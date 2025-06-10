{{ config(materialized = 'table') }}

WITH col AS (
    SELECT 
        hub_c.collection_hk,
        sat_c.collection_id
    FROM {{ref('hub_dspace5_collection')}} hub_c
    INNER JOIN {{ref('sat_dspace5_collection')}} sat_c
        ON sat_c.collection_hk = hub_c.collection_hk
)

SELECT * FROM col