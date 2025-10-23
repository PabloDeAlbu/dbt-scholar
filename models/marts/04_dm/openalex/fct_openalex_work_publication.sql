WITH
sat_work AS {{ latest_satellite(ref('sat_openalex_work'), 'work_hk') }},

final AS (
    SELECT
        dim_work.work_id,
        dim_work.work_hk,
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
    FROM {{ref('dim_openalex_work')}} dim_work
    INNER JOIN sat_work USING (work_hk)
)

SELECT * FROM final 
