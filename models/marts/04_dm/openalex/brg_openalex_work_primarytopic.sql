{{ config(materialized = 'table') }}

WITH base AS (
    SELECT
        hub_work.work_id,
        hub_topic.topic_id,
        primarytopic_score
    FROM {{ref('tlink_openalex_work_primarytopic')}} link
    JOIN {{ref('hub_openalex_work')}} hub_work USING (work_hk)
    JOIN {{ref('hub_openalex_topic')}} hub_topic USING (topic_hk)
)

SELECT DISTINCT * FROM base