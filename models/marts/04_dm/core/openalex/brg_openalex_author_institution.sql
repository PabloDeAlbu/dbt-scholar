{{ config(materialized = 'table') }}

WITH base AS (
    SELECT DISTINCT
        hub_author.author_id,
        hub_author.author_hk,
        hub_institution.institution_id,
        hub_institution.institution_hk,
        link_author_institution.author_institution_hk
    FROM {{ref('link_openalex_author_institution')}} link_author_institution
    INNER JOIN {{ref('hub_openalex_author')}} hub_author USING (author_hk)
    INNER JOIN {{ref('hub_openalex_institution')}} hub_institution USING (institution_hk)
)

SELECT * FROM base
