WITH base AS (
    SELECT 
        hub_work.work_id,
        hub_source.source_id,
        link_work_primarylocation_source.work_hk,
        link_work_primarylocation_source.source_hk,
        link_work_primarylocation_source.work_primarylocation_source_hk
    FROM {{ref('hub_openalex_work')}} hub_work
    INNER JOIN {{ref('link_openalex_work_primarylocation_source')}} link_work_primarylocation_source USING (work_hk)
    INNER JOIN {{ref('hub_openalex_source')}} hub_source USING (source_hk)
)

SELECT * FROM base
