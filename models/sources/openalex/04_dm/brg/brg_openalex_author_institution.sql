{{ config(materialized = 'table') }}

WITH base AS (
    SELECT 
        REPLACE(hub_author.author_id, 'https://openalex.org/', '') as author_id,
        REPLACE(hub_institution.institution_id, 'https://openalex.org/', '') as institution_id,
        COALESCE(REPLACE(hub_ror.ror, 'https://ror.org/', ''), '-') as ror
    FROM {{ref('link_openalex_author_institution')}} link_author_institution
    INNER JOIN {{ref('hub_openalex_author')}} hub_author ON link_author_institution.author_hk = hub_author.author_hk
    INNER JOIN {{ref('hub_openalex_institution')}} hub_institution ON link_author_institution.institution_hk = hub_institution.institution_hk
    INNER JOIN {{ref('sat_openalex_institution')}} sat_institution ON link_author_institution.institution_hk = sat_institution.institution_hk
    LEFT JOIN {{ref('link_openalex_institution_ror')}} link_institution_ror ON link_author_institution.institution_hk = link_institution_ror.institution_hk
    LEFT JOIN {{ref('hub_openalex_ror')}} hub_ror ON link_institution_ror.ror_hk = hub_ror.ror_hk
)

SELECT * FROM base
