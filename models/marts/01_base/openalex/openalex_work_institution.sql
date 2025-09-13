WITH base AS (
    SELECT 
        hub_work.work_id,
        hub_institution.institution_id,
        link_work_institution.work_hk,
        link_work_institution.institution_hk,
        link_work_institution.work_institution_hk
    FROM {{ref('hub_openalex_work')}} hub_work
    INNER JOIN {{ref('link_openalex_work_institution')}} link_work_institution USING (work_hk)
    INNER JOIN {{ref('hub_openalex_institution')}} hub_institution USING (institution_hk)
)

SELECT * FROM base
