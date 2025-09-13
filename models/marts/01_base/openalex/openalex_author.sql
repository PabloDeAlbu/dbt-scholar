WITH base AS (
    SELECT 
        hub_author.author_hk,
        hub_author.author_id,
        sat_author.display_name,
        works_count,
        cited_by_count
    FROM {{ref('hub_openalex_author')}} hub_author 
    INNER JOIN {{ref('sat_openalex_author')}} sat_author USING (author_hk)
)

SELECT * FROM base