
{{ config(materialized = 'table') }}

WITH base AS (
    SELECT 
        REPLACE(dim_w.work_id, 'https://openalex.org/', '') as work_id,
        REPLACE(dim_t.topic_id, 'https://openalex.org/', '') as topic_id,
        dim_t.display_name,
        dim_t.domain_display_name,
        dim_t.domain_id,
        dim_t.field_display_name,
        dim_t.field_id,
        dim_t.subfield_display_name,
        dim_t.subfield_id,
        brg.primarytopic_score,
        brg.topic_hk,
        brg.work_hk
    FROM {{ref('brg_openalex_work_primarytopic')}} brg
    INNER JOIN {{ref('dim_openalex_work')}} dim_w USING (work_hk)
    INNER JOIN {{ref('dim_openalex_topic')}} dim_t USING (topic_hk)
)

SELECT * FROM base
