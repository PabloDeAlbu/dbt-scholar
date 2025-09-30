{{ config(materialized = 'table') }}

WITH base AS (
    SELECT
        work_hk,
        institution_hk,
        work_institution_hk
    FROM {{ref('link_openalex_work_institution')}} link_work_institution
)

SELECT DISTINCT * FROM base