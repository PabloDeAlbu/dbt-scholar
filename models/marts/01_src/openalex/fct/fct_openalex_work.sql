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

openalex_work AS (
    SELECT 
        work_id,
        REPLACE(doi, 'https://doi.org/', '') AS doi,
        mag,
        pmcid,
        pmid,
        title,
        type,
        publication_year,
        publication_date,
        countries_distinct_count,
        institutions_distinct_count,
        fwci,
        has_fulltext,
        fulltext_origin,
        cited_by_count,
        is_retracted,
        is_paratext, 
        locations_count,
        referenced_works_count,
        any_repository_has_fulltext,
        is_oa,
        oa_status,
        oa_url,
        cited_by_percentile_year_max,
        cited_by_percentile_year_min,
        citation_normalized_percentile_is_in_top_10_percent,
        citation_normalized_percentile_is_in_top_1_percent,
        citation_normalized_percentile_value,
        apc_list_currency,
        apc_list_value,
        apc_list_value_usd,
        apc_paid_currency,
        apc_paid_value,
        apc_paid_value_usd,
        work_hk,
        doi_hk,
        mag_hk,
        pmcid_hk,
        pmid_hk
    FROM 
        base
),

join_coar AS (
    SELECT 
        openalex_work.*,
        coar.label_es as coar_type 
    FROM openalex_work
    INNER JOIN {{ref('seed_coar_openalex')}} coar ON
        openalex_work.type = coar.type
)

SELECT * FROM join_coar
