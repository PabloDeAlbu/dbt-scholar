WITH base as (
    SELECT
        sal_work.work_hk,
        sat_work.title,
        sat_work.type,
        sat_work.publication_year as publication_year,
        sat_work.publication_date as publication_date,
        sat_work.countries_distinct_count,
        sat_work.institutions_distinct_count,
        sat_work.fwci,
        sat_work.has_fulltext,
        sat_work.fulltext_origin,
        sat_work.cited_by_count,
        sat_work.is_retracted,
        sat_work.is_paratext, 
        sat_work.locations_count,
        sat_work.referenced_works_count,
        sat_work.any_repository_has_fulltext,
        sat_work.is_oa,
        sat_work.oa_status,
        sat_work.oa_url,
        sat_work.cited_by_percentile_year_max,
        sat_work.cited_by_percentile_year_min,
        sat_work.citation_normalized_percentile_is_in_top_10_percent,
        sat_work.citation_normalized_percentile_is_in_top_1_percent,
        sat_work.citation_normalized_percentile_value,
        sat_work.apc_list_currency,
        sat_work.apc_list_provenance,
        sat_work.apc_list_value,
        sat_work.apc_list_value_usd,
        sat_work.apc_paid_currency,
        sat_work.apc_paid_provenance,
        sat_work.apc_paid_value,
        sat_work.apc_paid_value_usd,
        hub_doi.doi,
        hub_mag.mag,
        hub_pmcid.pmcid,
        hub_pmid.pmid
    FROM {{ref('sal_openalex_work')}} sal_work
    INNER JOIN {{ref('sat_openalex_work')}} sat_work ON sat_work.work_hk = sal_work.work_hk
    INNER JOIN {{ref('hub_openalex_doi')}} hub_doi ON hub_doi.doi_hk = sal_work.doi_hk
    INNER JOIN {{ref('hub_openalex_mag')}} hub_mag ON hub_mag.mag_hk = sal_work.mag_hk
    INNER JOIN {{ref('hub_openalex_pmcid')}} hub_pmcid ON hub_pmcid.pmcid_hk = sal_work.pmcid_hk
    INNER JOIN {{ref('hub_openalex_pmid')}} hub_pmid ON hub_pmid.pmid_hk = sal_work.pmid_hk
)

SELECT * FROM base