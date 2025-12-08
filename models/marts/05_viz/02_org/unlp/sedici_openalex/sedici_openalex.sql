WITH base AS (
    SELECT DISTINCT
        w.work_id,
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
        w."has_pmid",
        w."countries_distinct_count",
        w."institutions_distinct_count",
        w."fwci",
        w."cited_by_count",
        w."locations_count",
        w."referenced_works_count",
        w."cited_by_percentile_year_max",
        w."cited_by_percentile_year_min",
        w."citation_normalized_percentile_is_in_top_10_percent",
        w."citation_normalized_percentile_is_in_top_1_percent",
        w."citation_normalized_percentile_value",
        w."apc_list_currency",
        w."apc_list_value",
        w."apc_list_value_usd",
        w."apc_paid_currency",
        w."apc_paid_value",
        w."apc_paid_value_usd",
        w_t."domain_display_name"
    FROM {{ref('viz_openalex_work')}}w
    LEFT JOIN {{ref('viz_openalex_work_primarytopic')}} w_t USING (work_hk)
)

SELECT 
* 
FROM base
ORDER BY any_repository_has_fulltext DESC, publication_year DESC, doi DESC