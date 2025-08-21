{# WITH openaire AS (
    SELECT * 
    FROM {{ref('fct_openaire_researchproduct')}}
),

openalex AS (
    SELECT * 
    FROM {{ref('fct_openalex_work')}}
),

final as (
    SELECT 
        openaire.doi,
        openaire.researchproduct_id,
        openalex.work_id,
        openaire.main_title,
        COALESCE(openalex.coar_type, openaire.coar_type) as coar_type,
        openaire.publication_date,
        openalex.title,
        openalex.publication_year,
        openalex.publication_date as openalex_publication_date,
        openaire.citation_count,
        openalex.cited_by_count,
        openaire.downloads,
        openaire.views,
        openalex.is_retracted,
        openalex.is_paratext,
        openalex.locations_count,
        openalex.referenced_works_count,
        openalex.any_repository_has_fulltext,
        openalex.is_oa,
        openalex.oa_status,
        openalex.oa_url,
        openalex.cited_by_percentile_year_max,
        openalex.cited_by_percentile_year_min,
        openalex.citation_normalized_percentile_is_in_top_10_percent,
        openalex.citation_normalized_percentile_is_in_top_1_percent,
        openalex.citation_normalized_percentile_value,
        openalex.apc_list_currency,
        openalex.apc_list_value,
        openalex.apc_list_value_usd,
        openalex.apc_paid_currency,
        openalex.apc_paid_value,
        openalex.apc_paid_value_usd
    FROM openaire
    INNER JOIN openalex ON
        openaire.doi = openalex.doi
    
)

SELECT * FROM final #}
