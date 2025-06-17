{{ config(materialized = 'table') }}

WITH base AS (
    SELECT 
        hub_institution.institution_hk,
        hub_institution.institution_id,
        hub_openalex_ror.ror,
        sat_institution.country_code,
        sat_institution.display_name
    FROM {{ref('hub_openalex_institution')}} hub_institution
    INNER JOIN {{ref('sat_openalex_institution')}} sat_institution ON sat_institution.institution_hk = hub_institution.institution_hk
    LEFT JOIN {{ref('link_openalex_institution_ror')}} link_institution_ror ON link_institution_ror.institution_hk = hub_institution.institution_hk
    LEFT JOIN {{ref('hub_openalex_ror')}} hub_openalex_ror ON hub_openalex_ror.ror_hk = link_institution_ror.ror_hk
)

SELECT * FROM base