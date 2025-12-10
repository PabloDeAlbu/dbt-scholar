WITH base AS (
    SELECT DISTINCT
        w.work_id,
        w_i.institution_id,
        w_i."ror",
        w_i."institution_display_name",
        w."title",
        w."type",
        w."publication_year",
        w."publication_date",
        w."has_fulltext",
        w."fulltext_origin",
        w."is_retracted",
        w."is_paratext",
        w."any_repository_has_fulltext",
        w."is_oa",
        w."oa_status",
        w."oa_url",
        w."doi",
        w."mag",
        w."pmcid",
        w."pmid",
        w."has_doi",
        w."has_mag",
        w."has_pmcid",
        w."has_pmid"
    FROM {{ref('viz_openalex_work')}}w
    INNER JOIN {{ref('viz_openalex_work_institution')}} w_i USING (work_hk)
)

SELECT 
* 
FROM base
ORDER BY any_repository_has_fulltext DESC, publication_year DESC, doi DESC