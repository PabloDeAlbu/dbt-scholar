WITH base AS (
    SELECT 
        hub.topic_id,
        sat.topic_display_name,
        sat.topic_domain_display_name,
        sat.topic_domain_id,
        sat.topic_field_display_name,
        sat.topic_field_id,
        sat.topic_subfield_display_name,
        sat.topic_subfield_id
    FROM {{ref('hub_openalex_topic')}} hub
    JOIN {{ref('sat_openalex_topic')}} sat USING (topic_hk)
)

SELECT * FROM base