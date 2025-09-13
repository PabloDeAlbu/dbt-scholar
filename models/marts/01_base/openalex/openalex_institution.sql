WITH base AS (
    SELECT 
        hub_institution.institution_hk,
        hub_institution.institution_id,
        sat_institution.country_code,
        sat_institution.display_name
    FROM {{ref('hub_openalex_institution')}} hub_institution 
    INNER JOIN {{ref('sat_openalex_institution')}} sat_institution USING (institution_hk)
)

SELECT * FROM base