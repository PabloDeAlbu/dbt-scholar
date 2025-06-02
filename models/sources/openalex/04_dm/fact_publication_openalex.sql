WITH base as (
    SELECT
        hub_work.work_id,
        hub_doi.doi,
        hub_mag.mag,
        hub_pmcid.pmcid,
        hub_pmid.pmid,
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
        sat_work.apc_list_value,
        sat_work.apc_list_value_usd,
        sat_work.apc_paid_currency,
        sat_work.apc_paid_value,
        sat_work.apc_paid_value_usd,
        hub_work.work_hk,
        hub_doi.doi_hk,
        hub_mag.mag_hk,
        hub_pmcid.pmcid_hk,
        hub_pmid.pmid_hk
    FROM {{ref('hub_openalex_work')}} hub_work
    INNER JOIN {{ref('sat_openalex_work')}} sat_work ON sat_work.work_hk = hub_work.work_hk
    LEFT JOIN {{ref('link_openalex_work_doi')}} link_work_doi ON link_work_doi.work_hk = hub_work.work_hk
    LEFT JOIN {{ref('hub_openalex_doi')}} hub_doi ON hub_doi.doi_hk = link_work_doi.doi_hk
    LEFT JOIN {{ref('link_openalex_work_mag')}} link_work_mag ON link_work_mag.work_hk = hub_work.work_hk
    LEFT JOIN {{ref('hub_openalex_mag')}} hub_mag ON hub_mag.mag_hk = link_work_mag.mag_hk
    LEFT JOIN {{ref('link_openalex_work_pmcid')}} link_work_pmcid ON link_work_pmcid.work_hk = hub_work.work_hk
    LEFT JOIN {{ref('hub_openalex_pmcid')}} hub_pmcid ON hub_pmcid.pmcid_hk = link_work_pmcid.pmcid_hk
    LEFT JOIN {{ref('link_openalex_work_pmid')}} link_work_pmid ON link_work_pmid.work_hk = hub_work.work_hk
    LEFT JOIN {{ref('hub_openalex_pmid')}} hub_pmid ON hub_pmid.pmid_hk = link_work_pmid.pmid_hk
),

fillna as (
    SELECT 
        work_id,
        COALESCE(doi, 'NO DATA') as doi,
        COALESCE(mag, 'NO DATA') as mag,
        COALESCE(pmcid, 'NO DATA') as pmcid,
        COALESCE(pmid, 'NO DATA') as pmid,
        COALESCE(title, 'NO DATA') as title,
        COALESCE(type, 'NO DATA') as type,
        COALESCE(publication_year, 0) as publication_year,
        publication_date,
        COALESCE(countries_distinct_count, 0) as countries_distinct_count,
        COALESCE(institutions_distinct_count, 0) as institutions_distinct_count,
        COALESCE(fwci, 0) as fwci,
        COALESCE(has_fulltext, False) as has_fulltext,
        COALESCE(fulltext_origin, 'NO DATA') as fulltext_origin,
        COALESCE(cited_by_count, 0) as cited_by_count,
        COALESCE(is_retracted, False) as is_retracted,
        COALESCE(is_paratext, False) as is_paratext,
        COALESCE(locations_count, 0) as locations_count,
        COALESCE(referenced_works_count, 0) as referenced_works_count,
        COALESCE(any_repository_has_fulltext, False) as any_repository_has_fulltext,
        COALESCE(is_oa, False) as is_oa,
        COALESCE(oa_status, 'NO DATA') as oa_status,
        COALESCE(oa_url, 'NO DATA') as oa_url,
        COALESCE(cited_by_percentile_year_max, 0) as cited_by_percentile_year_max,
        COALESCE(cited_by_percentile_year_min, 0) as cited_by_percentile_year_min,
        COALESCE(citation_normalized_percentile_is_in_top_10_percent, False) as citation_normalized_percentile_is_in_top_10_percent,
        COALESCE(citation_normalized_percentile_is_in_top_1_percent, False) as citation_normalized_percentile_is_in_top_1_percent,
        COALESCE(citation_normalized_percentile_value, 0) as citation_normalized_percentile_value,
        COALESCE(apc_list_currency, 'NO DATA') as apc_list_currency,
        COALESCE(apc_list_value, 0) as apc_list_value,
        COALESCE(apc_list_value_usd, 0) as apc_list_value_usd,
        COALESCE(apc_paid_currency, 'NO DATA') as apc_paid_currency,
        COALESCE(apc_paid_value, 0) as apc_paid_value,
        COALESCE(apc_paid_value_usd, 0) as apc_paid_value_usd,
        work_hk,
        doi_hk,
        mag_hk,
        pmcid_hk,
        pmid_hk
    FROM base
)

SELECT * FROM fillna