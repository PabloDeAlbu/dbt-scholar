WITH latest_sat_work AS (
    SELECT *
    FROM {{ ref('latest_sat_openalex_work') }}
),

final AS (
    SELECT
        hub_work.work_id,
        hub_work.work_hk,
        sat_work.title,
        sat_work.type,
        CASE
            WHEN sat_work.publication_year IS NOT NULL THEN TO_DATE(sat_work.publication_year::text, 'YYYY')
        END AS publication_year,
        sat_work.publication_date,
        sat_work.language,
        sat_work.has_fulltext,
        sat_work.fulltext_origin,
        sat_work.is_retracted,
        sat_work.is_paratext,
        sat_work.any_repository_has_fulltext,
        sat_work.is_oa,
        sat_work.oa_status,
        sat_work.oa_url,
        sat_work.biblio_first_page,
        sat_work.biblio_issue,
        sat_work.biblio_last_page,
        sat_work.biblio_volume,
        sat_work.countries_distinct_count,
        sat_work.institutions_distinct_count,
        sat_work.fwci,
        sat_work.cited_by_count,
        sat_work.locations_count,
        sat_work.referenced_works_count,
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
        sat_work.apc_paid_value_usd
    FROM {{ ref('hub_openalex_work') }} hub_work
    INNER JOIN latest_sat_work sat_work USING (work_hk)
)

SELECT * FROM final
