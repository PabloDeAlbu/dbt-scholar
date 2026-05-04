
{{ config(materialized = 'table') }}

WITH base AS (
    SELECT 
        REPLACE(work.work_id, 'https://openalex.org/', '') as work_id,
        REPLACE(COALESCE(brg.author_id, dim_a.author_id), 'https://openalex.org/', '') as author_id,
        COALESCE(brg.canonical_display_name, brg.fallback_display_name) as display_name,
        COALESCE(dim_a.orcid, brg.fallback_orcid, '-') as orcid,
        CASE
            WHEN COALESCE(dim_a.orcid, brg.fallback_orcid) IS NOT NULL THEN TRUE
            ELSE FALSE
        END as has_orcid,
        dim_a.works_count,
        dim_a.cited_by_count,
        brg.work_hk,
        brg.author_hk
    FROM {{ref('brg_openalex_work_author_enriched')}} brg
    INNER JOIN {{ref('fct_openalex_work_publication')}} work USING (work_hk)
    LEFT JOIN {{ref('dim_openalex_author')}} dim_a USING (author_hk)
    WHERE COALESCE(brg.canonical_display_name, brg.fallback_display_name) IS NOT NULL
)

SELECT * FROM base
