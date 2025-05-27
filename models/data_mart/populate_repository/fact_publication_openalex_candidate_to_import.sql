WITH fact_publication_ir AS (
    SELECT 
        *
    FROM {{ref('fact_publication_dspace5')}}
),

fact_publication_openalex AS (
    SELECT 
        *
    FROM {{ref('fact_publication_openalex')}}
),

dim_doi_ir AS (
    SELECT *
    FROM {{ref('dim_doi_dspace5')}}
)

SELECT 
    fact_publication_openalex.work_id,
    fact_publication_openalex.doi,
    fact_publication_openalex.mag,
    fact_publication_openalex.pmcid,
    fact_publication_openalex.pmid,
    fact_publication_openalex.title,
    fact_publication_openalex.type,
    fact_publication_openalex.publication_year,
    fact_publication_openalex.publication_date,
    fact_publication_openalex.countries_distinct_count,
    fact_publication_openalex.institutions_distinct_count,
    fact_publication_openalex.fwci,
    fact_publication_openalex.has_fulltext,
    fact_publication_openalex.fulltext_origin,
    fact_publication_openalex.cited_by_count,
    fact_publication_openalex.is_retracted,
    fact_publication_openalex.is_paratext,
    fact_publication_openalex.locations_count,
    fact_publication_openalex.referenced_works_count,
    fact_publication_openalex.any_repository_has_fulltext,
    fact_publication_openalex.is_oa,
    fact_publication_openalex.oa_status,
    fact_publication_openalex.oa_url,
    fact_publication_openalex.cited_by_percentile_year_max,
    fact_publication_openalex.cited_by_percentile_year_min,
    fact_publication_openalex.citation_normalized_percentile_is_in_top_10_percent,
    fact_publication_openalex.citation_normalized_percentile_is_in_top_1_percent,
    fact_publication_openalex.citation_normalized_percentile_value,
    fact_publication_openalex.apc_list_currency,
    fact_publication_openalex.apc_list_provenance,
    fact_publication_openalex.apc_list_value,
    fact_publication_openalex.apc_list_value_usd,
    fact_publication_openalex.apc_paid_currency,
    fact_publication_openalex.apc_paid_provenance,
    fact_publication_openalex.apc_paid_value,
    fact_publication_openalex.apc_paid_value_usd
FROM fact_publication_openalex
LEFT JOIN dim_doi_ir ON fact_publication_openalex.doi_hk = dim_doi_ir.doi_hk
LEFT JOIN {{ref('bridge_publication_doi_dspace5')}} ON bridge_publication_doi_dspace5.doi_hk = fact_publication_openalex.doi_hk
LEFT JOIN fact_publication_ir ON fact_publication_ir.item_hk = bridge_publication_doi_dspace5.item_hk
where dim_doi_ir.doi is null
