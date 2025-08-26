{{ config(materialized = 'table') }}

WITH base AS (
    SELECT 
        hub_work.work_id,        
        hub_institution.institution_id,
        COALESCE(sat_institution.display_name, '-') as institution_display_name,
        COALESCE(hub_ror.ror, '-') as ror
    FROM {{ref('link_openalex_work_institution')}} link_work_institution
    INNER JOIN {{ref('hub_openalex_work')}} hub_work ON link_work_institution.work_hk = hub_work.work_hk
    INNER JOIN {{ref('hub_openalex_institution')}} hub_institution ON link_work_institution.institution_hk = hub_institution.institution_hk
    INNER JOIN {{ref('sat_openalex_institution')}} sat_institution ON link_work_institution.institution_hk = sat_institution.institution_hk
    LEFT JOIN {{ref('link_openalex_institution_ror')}} link_institution_ror ON link_work_institution.institution_hk = link_institution_ror.institution_hk
    LEFT JOIN {{ref('hub_openalex_ror')}} hub_ror ON link_institution_ror.ror_hk = hub_ror.ror_hk
)

SELECT * FROM base