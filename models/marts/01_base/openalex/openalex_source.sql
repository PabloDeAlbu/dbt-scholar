WITH base AS (
    SELECT 
        hub_source.source_hk,
        hub_source.source_id,
        sat_source.source_display_name as source_display_name,
        sat_source.source_type as type,
        sat_source.source_host_organization as host_organization,
        sat_source.source_host_organization_name as host_organization_name,
        sat_source.source_is_core as is_core,
        sat_source.source_is_in_doaj as is_in_doaj,
        sat_source.source_is_oa as is_oa
    FROM {{ref('hub_openalex_source')}} hub_source 
    INNER JOIN {{ref('sat_openalex_source')}} sat_source USING (source_hk)
)

SELECT * FROM base