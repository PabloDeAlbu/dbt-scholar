WITH base AS (
    SELECT 
        REPLACE(author_id, 'https://openalex.org/', '') as author_id,
        display_name,
        works_count,
        cited_by_count,
        orcid,
        has_orcid
    FROM {{ref('dim_openalex_author')}}
)

SELECT * FROM base
