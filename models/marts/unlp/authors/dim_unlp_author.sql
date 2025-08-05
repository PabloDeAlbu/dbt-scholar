with openalex_authors AS (
    SELECT 
        author_id as openalex_author_id,
        display_name as openalex_display_name,
        works_count as openalex_works_count,
        cited_by_count as openalex_cited_by_count,
        REPLACE(orcid, 'https://orcid.org/','') AS orcid,
        author_hk as openalex_author_hk
    FROM {{ref('dim_openalex_author')}}
),

openaire_authors AS (
    SELECT *
    FROM (
        SELECT 
            REPLACE(orcid, 'https://orcid.org/','') as orcid,
            full_name,
            name as openaire_name,
            surname as openaire_surname,
            ROW_NUMBER() OVER (PARTITION BY REPLACE(orcid, 'https://orcid.org/','') ORDER BY full_name) AS rn
        FROM {{ref('dim_openaire_author')}}
        WHERE orcid IS NOT NULL
    ) sub
    WHERE rn = 1
),

final AS (
    SELECT 
        openalex.openalex_author_id,
        openalex.openalex_display_name,
        openalex.openalex_works_count,
        openalex.openalex_cited_by_count,
        openalex.orcid
    FROM openalex_authors AS openalex
    INNER JOIN openaire_authors AS openaire ON openaire.orcid = openalex.orcid
)

SELECT * FROM final
