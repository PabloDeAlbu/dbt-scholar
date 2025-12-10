{{ config(materialized = 'table') }}

WITH base AS (
    SELECT DISTINCT
        hub_work.work_id,
        hub_work.work_hk,
        hub_topic.topic_id,
        hub_topic.topic_hk,
        link.primarytopic_score
    FROM {{ref('tlink_openalex_work_primarytopic')}} link
    INNER JOIN {{ref('hub_openalex_work')}} hub_work USING (work_hk)
    INNER JOIN {{ref('hub_openalex_topic')}} hub_topic USING (topic_hk)
)

SELECT * FROM base
