WITH base AS (
    SELECT 
        hub_topic.topic_hk,
        hub_topic.topic_id,
        sat_topic.display_name,
        sat_topic.topic_id,
        sat_topic.domain_display_name,
        sat_topic.domain_id,
        sat_topic.field_display_name,
        sat_topic.field_id,
        sat_topic.subfield_display_name,
        sat_topic.subfield_id
    FROM {{ref('hub_openalex_topic')}} hub_topic 
    INNER JOIN {{ref('sat_openalex_topic')}} sat_topic USING (topic_hk)
)

SELECT * FROM base