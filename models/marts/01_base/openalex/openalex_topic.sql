WITH base AS (
    SELECT 
        hub_topic.topic_hk,
        hub_topic.topic_id,
        sat_topic.topic_display_name as display_name,
        sat_topic.topic_domain_display_name as domain_display_name,
        sat_topic.topic_domain_id as domain_id,
        sat_topic.topic_field_display_name as field_display_name,
        sat_topic.topic_field_id as field_id,
        sat_topic.topic_subfield_display_name as subfield_display_name,
        sat_topic.topic_subfield_id as subfield_id
    FROM {{ref('hub_openalex_topic')}} hub_topic 
    INNER JOIN {{ref('sat_openalex_topic')}} sat_topic USING (topic_hk)
)

SELECT * FROM base