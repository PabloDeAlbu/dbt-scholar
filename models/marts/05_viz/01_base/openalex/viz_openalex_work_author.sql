
{{ config(materialized = 'table') }}

WITH base AS (
    SELECT 
        REPLACE(dim_w.work_id, 'https://openalex.org/', '') as work_id,
        REPLACE(dim_a.author_id, 'https://openalex.org/', '') as author_id,
        dim_a.display_name,
        COALESCE(orcid, '-') as orcid,
        brg.work_hk,
        brg.author_hk
    FROM {{ref('brg_openalex_work_author')}} brg
    INNER JOIN {{ref('dim_openalex_work')}} dim_w USING (work_hk)
    INNER JOIN {{ref('dim_openalex_author')}} dim_a USING (author_hk)
)

SELECT * FROM base
