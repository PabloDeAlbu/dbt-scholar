{{ config(materialized = 'table') }}

WITH base AS (
    SELECT 
        author_hk,
        institution_hk,
        author_institution_hk
    FROM {{ref('link_openalex_author_institution')}} link_author_institution
)

SELECT * FROM base
