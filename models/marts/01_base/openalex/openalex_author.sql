WITH base AS (
    SELECT 
        hub_author.author_id,
        sat_author.display_name,
        works_count,
        cited_by_count,
        hub_orcid.orcid,
        hub_author.author_hk
    FROM {{ref('hub_openalex_author')}} hub_author 
    INNER JOIN {{ref('sat_openalex_author')}} sat_author USING (author_hk)
    LEFT JOIN {{ref('link_openalex_author_orcid')}} link_author_orcid USING (author_hk)
    LEFT JOIN {{ref('hub_openalex_orcid')}} hub_orcid USING (orcid_hk)
)

SELECT * FROM base