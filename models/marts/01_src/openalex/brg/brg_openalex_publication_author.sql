{{ config(materialized = 'table') }}

WITH base AS (
    SELECT 
        link_work_author.work_author_hk,
        link_work_author.work_hk,
        link_work_author.author_hk
    FROM {{ref('link_openalex_work_author')}} link_work_author
)

SELECT * FROM base