WITH base as (
    SELECT
        hub_work.work_id,
        hub_work.work_hk
    FROM {{ref('hub_openalex_work')}} hub_work
    INNER JOIN {{ref('sat_openalex_work')}} sat_work ON hub_work.work_hk = sat_work.work_hk 
)

SELECT * FROM base
