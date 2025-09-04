{{ config(materialized = 'table') }}

WITH base AS (
    SELECT DISTINCT
        REPLACE(hub_institution.institution_id, 'https://openalex.org/', '') as institution_id,
        COALESCE(REPLACE(hub_ror.ror, 'https://ror.org/', ''), '-') as ror,
        sat_institution.display_name as institution_display_name,
        COALESCE(sat_institution.country_code, '-') as country_code
    FROM {{ref('hub_openalex_institution')}} hub_institution
    INNER JOIN {{ref('sat_openalex_institution')}} sat_institution ON sat_institution.institution_hk = hub_institution.institution_hk
    LEFT JOIN {{ref('link_openalex_institution_ror')}} link_institution_ror ON link_institution_ror.institution_hk = hub_institution.institution_hk
    LEFT JOIN {{ref('hub_openalex_ror')}} hub_ror ON hub_ror.ror_hk = link_institution_ror.ror_hk
)

SELECT * FROM base
