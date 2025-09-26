{{ config(materialized = 'table') }}

WITH base AS (
    SELECT
        REPLACE(hub_work.work_id, 'https://openalex.org/', '') as work_id,
        REPLACE(hub_institution.institution_id, 'https://openalex.org/', '') as institution_id,
        COALESCE(sat_institution.display_name, '-') as institution_display_name,
        COALESCE(REPLACE(hub_ror.ror, 'https://ror.org/', ''), '-') as ror
    FROM {{ref('link_openalex_work_institution')}} link_work_institution
    JOIN {{ref('hub_openalex_work')}} hub_work USING (work_hk)
    JOIN {{ref('hub_openalex_institution')}} hub_institution USING (institution_hk)
    JOIN {{ref('sat_openalex_institution')}} sat_institution USING (institution_hk)
    LEFT JOIN {{ref('link_openalex_institution_ror')}} USING(institution_hk)
    LEFT JOIN {{ref('hub_openalex_ror')}} hub_ror USING(ror_hk)
)

SELECT DISTINCT * FROM base