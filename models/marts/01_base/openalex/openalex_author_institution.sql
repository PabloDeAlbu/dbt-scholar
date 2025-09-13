WITH base AS (
    SELECT 
        link_author_institution.author_hk,
        link_author_institution.institution_hk,
        sat_affiliation.years
    FROM {{ref('link_openalex_author_institution')}}  link_author_institution
    INNER JOIN {{ref('sat_openalex_affiliation')}} sat_affiliation USING (author_institution_hk)
)

SELECT * FROM base