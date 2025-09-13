WITH base AS (
    SELECT 
        hub_work.work_id,
        hub_source.source_id,
        link_work_locations.work_hk,
        link_work_locations.source_hk,
        link_work_locations.work_locations_hk
    FROM {{ref('hub_openalex_work')}} hub_work
    INNER JOIN {{ref('link_openalex_work_locations')}} link_work_locations USING (work_hk)
    INNER JOIN {{ref('hub_openalex_source')}} hub_source USING (source_hk)
)

SELECT * FROM base
