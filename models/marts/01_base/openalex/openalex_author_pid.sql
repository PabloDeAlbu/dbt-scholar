WITH base AS (
    SELECT 
        author.author_hk,
        author.author_id,
        hub_orcid.orcid
    FROM {{ref('openalex_author')}} author 
    LEFT JOIN {{ref('link_openalex_author_orcid')}} link_author_orcid USING (author_hk)
    LEFT JOIN {{ref('hub_openalex_orcid')}} hub_orcid USING (orcid_hk)
)

SELECT * FROM base
