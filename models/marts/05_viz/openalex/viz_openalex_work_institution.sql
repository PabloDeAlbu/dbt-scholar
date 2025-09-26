
{{ config(materialized = 'table') }}

WITH base AS (
    SELECT 
        REPLACE(work_id, 'https://openalex.org/', '') as work_id,
        REPLACE(institution_id, 'https://openalex.org/', '') as institution_id,
        COALESCE(REPLACE(ror, 'https://ror.org/', ''), '-') as ror,
        brg.institution_hk,
        brg.dim_work.work_hk
    FROM {{ref('brg_openalex_work_institution')}} brg
    INNER JOIN {{ref('dim_openalex_work')}} USING (work_hk)
    INNER JOIN {{ref('dim_openalex_institution')}} USING (institution_hk)
)

SELECT * FROM base
