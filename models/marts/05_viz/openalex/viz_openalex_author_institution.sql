{{ config(materialized = 'table') }}

WITH base AS (
    SELECT 
        REPLACE(author_id, 'https://openalex.org/', '') as author_id,
        REPLACE(institution_id, 'https://openalex.org/', '') as institution_id,
        COALESCE(REPLACE(ror, 'https://ror.org/', ''), '-') as ror,
        author_hk,
        institution_hk
    FROM {{ref('brg_openalex_author_institution')}} 
    INNER JOIN {{ref('dim_openalex_author')}} USING (author_hk)
    INNER JOIN {{ref('dim_openalex_institution')}} USING (institution_hk)
)

SELECT * FROM base
