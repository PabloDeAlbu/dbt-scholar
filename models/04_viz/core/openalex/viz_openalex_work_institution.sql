
{{ config(materialized = 'table') }}

WITH base AS (
    SELECT 
        REPLACE(work.work_id, 'https://openalex.org/', '') as work_id,
        REPLACE(dim_i.institution_id, 'https://openalex.org/', '') as institution_id,
        COALESCE(REPLACE(ror, 'https://ror.org/', ''), '-') as ror,
        dim_i.institution_display_name,
        brg.institution_hk,
        brg.work_hk
    FROM {{ref('brg_openalex_work_institution')}} brg
    INNER JOIN {{ref('fct_openalex_work_publication')}} work USING (work_hk)
    INNER JOIN {{ref('dim_openalex_institution')}} dim_i USING (institution_hk)
)

SELECT * FROM base
