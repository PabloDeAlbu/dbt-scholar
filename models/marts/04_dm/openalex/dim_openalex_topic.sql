WITH base AS (
    SELECT 
        hub.topic_id,
        sat.display_name,
        sat.domain_display_name,
        sat.domain_id,
        sat.field_display_name,
        sat.field_id,
        sat.subfield_display_name,
        sat.subfield_id
    FROM {{ref('hub_openalex_topic')}} hub
    JOIN {{ref('sat_openalex_topic')}} sat USING (topic_hk)
)

SELECT * FROM base