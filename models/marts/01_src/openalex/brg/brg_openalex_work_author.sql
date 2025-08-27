{{ config(materialized = 'table') }}

WITH base AS (
    SELECT 
        REPLACE(hub_work.work_id, 'https://openalex.org/','') as work_id,
        REPLACE(hub_author.author_id, 'https://openalex.org/', '') as author_id,
        COALESCE(sat_author.display_name, '-') as author_display_name,
        COALESCE(REPLACE(hub_orcid.orcid, 'https://orcid.org/', ''), '-') as orcid
    FROM {{ref('link_openalex_work_author')}} link_work_author
    INNER JOIN {{ref('hub_openalex_work')}} hub_work ON link_work_author.work_hk = hub_work.work_hk
    INNER JOIN {{ref('hub_openalex_author')}} hub_author ON link_work_author.author_hk = hub_author.author_hk
    INNER JOIN {{ref('sat_openalex_author')}} sat_author ON link_work_author.author_hk = sat_author.author_hk
    LEFT JOIN {{ref('link_openalex_author_orcid')}} link_author_orcid ON link_work_author.author_hk = link_author_orcid.author_hk
    LEFT JOIN {{ref('hub_openalex_orcid')}} hub_orcid ON link_author_orcid.orcid_hk = hub_orcid.orcid_hk
)

SELECT * FROM base