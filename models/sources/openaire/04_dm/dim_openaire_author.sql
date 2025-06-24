{{ config(materialized = 'table') }}

WITH base AS (
    SELECT 
        sat_author.full_name,
        sat_author.orcid,
        sat_author.name,
        sat_author.surname,
        sat_author.author_hk
    FROM {{ref('sat_openaire_author')}} sat_author
)

SELECT * FROM base