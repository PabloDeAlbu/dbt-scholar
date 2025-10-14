{{ config(materialized = 'table') }}

WITH base AS (
    SELECT 
        REPLACE(dim_a.author_id, 'https://openalex.org/', '') as author_id,
        REPLACE(dim_i.institution_id, 'https://openalex.org/', '') as institution_id,
        COALESCE(REPLACE(ror, 'https://ror.org/', ''), '-') as ror,
        dim_a.author_hk,
        dim_i.institution_hk
    FROM {{ref('brg_openalex_author_institution')}} 
    INNER JOIN {{ref('dim_openalex_author')}} dim_a USING (author_hk)
    INNER JOIN {{ref('dim_openalex_institution')}} dim_i USING (institution_hk)
)

SELECT * FROM base
