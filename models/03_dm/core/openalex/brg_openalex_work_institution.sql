{{ config(materialized = 'table') }}

WITH base AS (
    SELECT DISTINCT
        hub_work.work_id,
        hub_work.work_hk,
        hub_institution.institution_id,
        hub_institution.institution_hk,
        link_work_institution.work_institution_hk
    FROM {{ref('link_openalex_work_institution')}} link_work_institution
    INNER JOIN {{ref('hub_openalex_work')}} hub_work USING (work_hk)
    INNER JOIN {{ref('hub_openalex_institution')}} hub_institution USING (institution_hk)
)

SELECT * FROM base
