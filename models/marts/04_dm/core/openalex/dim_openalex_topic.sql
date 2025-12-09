WITH topic AS (
    SELECT
        hub_topic.topic_id,
        hub_topic.topic_hk,
        sat_topic.display_name,
        sat_topic.domain_display_name,
        sat_topic.domain_id,
        sat_topic.field_display_name,
        sat_topic.field_id,
        sat_topic.subfield_display_name,
        sat_topic.subfield_id
    FROM {{ref('hub_openalex_topic')}} hub_topic
    INNER JOIN {{ latest_satellite(ref('sat_openalex_topic'), 'topic_hk') }} AS sat_topic USING (topic_hk)
),

dim AS (
    SELECT
        topic.topic_id,
        topic.topic_hk,
        topic.display_name,
        topic.domain_display_name,
        topic.domain_id,
        topic.field_display_name,
        topic.field_id,
        topic.subfield_display_name,
        topic.subfield_id
    FROM topic
)

SELECT * FROM dim
