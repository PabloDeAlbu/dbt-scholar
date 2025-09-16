WITH base AS (
    SELECT 
        hub_work.work_id as id,
        sat_work.title,
        {# as subtitle, #}
        sat_work.type,
        sat_work.publication_date as date,
        REPLACE(hub_doi.doi,'https://doi.org/','') as doi,
        STRING_AGG(sat_author.display_name, '|') AS author
    FROM {{ref('hub_openalex_work')}} hub_work 
    INNER JOIN {{ref('sat_openalex_work')}} sat_work USING (work_hk)
    LEFT JOIN {{ref('link_openalex_work_doi')}} link_work_doi USING (work_hk)
    LEFT JOIN {{ref('hub_openalex_doi')}} hub_doi USING (doi_hk)
    LEFT JOIN {{ref('link_openalex_work_author')}} link_work_author USING (work_hk)
    LEFT JOIN {{ref('sat_openalex_author')}} sat_author USING (author_hk)
    GROUP BY 1, 2, 3, 4, 5
)

SELECT * FROM base