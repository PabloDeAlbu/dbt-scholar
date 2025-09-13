WITH base AS (
    SELECT
        hub_work.work_id,
        hub_author.author_id,
        hub_work.work_hk,
        hub_author.author_hk,        
        link_work_author.work_author_hk
    FROM {{ref('hub_openalex_work')}} hub_work
    INNER JOIN {{ref('link_openalex_work_author')}} link_work_author USING (work_hk)
    INNER JOIN {{ref('hub_openalex_author')}} hub_author USING (author_hk)
)

SELECT * FROM base
