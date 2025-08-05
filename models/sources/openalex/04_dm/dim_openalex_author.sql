{{ config(materialized = 'table') }}

WITH base AS (
    SELECT 
        hub_author.author_hk,
        hub_author.author_id,
        sat_author.display_name,
        sat_author.works_count,
        sat_author.cited_by_count,
        hub_orcid.orcid
    FROM {{ref('hub_openalex_author')}} hub_author
    INNER JOIN {{ref('sat_openalex_author')}} sat_author ON hub_author.author_hk = sat_author.author_hk
    LEFT JOIN {{ref('link_openalex_author_orcid')}} lnk_author_orcid ON hub_author.author_hk = lnk_author_orcid.author_hk
    LEFT JOIN {{ref('hub_openalex_orcid')}} hub_orcid ON hub_orcid.orcid_hk = lnk_author_orcid.orcid_hk
)

SELECT * FROM base