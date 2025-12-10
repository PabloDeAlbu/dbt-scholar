{{ config(materialized = 'table') }}

WITH base AS (
    SELECT 
        hub.coar_uri,
        sat.label_es,
        sat.label,
        sat.parent_label_1,
        sat.parent_label_2,
        sat.parent_label_3
    FROM {{ref('hub_coar_resourcetype')}} hub
    INNER JOIN  {{ref('sat_coar_resourcetype')}} sat ON
        hub.coar_hk = sat.coar_hk
)

SELECT * FROM base