WITH base AS (
    SELECT 
        link_author_institution.author_institution_hk,
        link_author_institution.author_hk,
        link_author_institution.institution_hk
    FROM {{ref('link_openalex_author_institution')}} link_author_institution
)

SELECT * FROM base