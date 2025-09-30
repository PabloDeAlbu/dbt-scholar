{{ config(materialized = 'table') }}

WITH base AS (
    SELECT DISTINCT
        hub_work.work_id,
        hub_work.work_hk,
        hub_author.author_id,
        hub_author.author_hk,
        link_work_author.work_author_hk
    FROM {{ref('link_openalex_work_author')}} link_work_author
    INNER JOIN {{ref('hub_openalex_work')}} hub_work USING (work_hk)
    INNER JOIN {{ref('hub_openalex_author')}} hub_author USING (author_hk)
)

SELECT * FROM base
