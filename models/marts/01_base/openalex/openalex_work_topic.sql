WITH base AS (
    SELECT
        {# hub_work.work_id, #}
        {# hub_topic.topic_id, #}
        sat_topic.*
        {# hub_work.work_hk, #}
        {# hub_topic.topic_hk,         #}
        {# link_work_topic.work_topic_hk #}
    FROM {{ref('hub_openalex_work')}} hub_work
    INNER JOIN {{ref('link_openalex_work_topic')}} link_work_topic USING (work_hk)
    INNER JOIN {{ref('hub_openalex_topic')}} hub_topic USING (topic_hk)
    INNER JOIN {{ref('sat_openalex_topic')}} sat_topic USING (topic_hk)
)

SELECT * FROM base
