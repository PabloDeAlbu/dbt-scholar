{%- set unlp_ror = 'https://ror.org/01tjs6929' -%}

WITH unlp_work_seen AS (
    SELECT
        work_hk,
        institution_hk,
        MAX(ror) AS institution_ror,
        MIN(load_datetime) AS unlp_first_seen_datetime,
        MAX(load_datetime) AS unlp_last_seen_datetime
    FROM {{ ref('stg_openalex_work_institution') }}
    WHERE ror = '{{ unlp_ror }}'
    GROUP BY work_hk, institution_hk
),

base AS (
    SELECT
        fct.work_hk,
        fct.work_id,
        unlp_work_seen.institution_hk,
        dim_institution.institution_id,
        dim_institution.institution_display_name,
        unlp_work_seen.institution_ror,
        unlp_work_seen.unlp_first_seen_datetime,
        unlp_work_seen.unlp_last_seen_datetime,

        dim_work.title,
        dim_work.type,
        dim_work.publication_year,
        dim_work.publication_date,
        dim_work.has_fulltext,
        dim_work.fulltext_origin,
        dim_work.is_retracted,
        dim_work.is_paratext,
        dim_work.any_repository_has_fulltext,
        dim_work.is_oa,
        dim_work.oa_status,
        dim_work.oa_url,
        dim_work.doi,
        dim_work.mag,
        dim_work.pmcid,
        dim_work.pmid,
        dim_work.has_doi,
        dim_work.has_mag,
        dim_work.has_pmcid,
        dim_work.has_pmid,

        fct.countries_distinct_count,
        fct.institutions_distinct_count,
        fct.fwci,
        fct.cited_by_count,
        fct.locations_count,
        fct.referenced_works_count,
        fct.cited_by_percentile_year_max,
        fct.cited_by_percentile_year_min,
        fct.citation_normalized_percentile_is_in_top_10_percent,
        fct.citation_normalized_percentile_is_in_top_1_percent,
        fct.citation_normalized_percentile_value,
        fct.apc_list_currency,
        fct.apc_list_value,
        fct.apc_list_value_usd,
        fct.apc_paid_currency,
        fct.apc_paid_value,
        fct.apc_paid_value_usd
    FROM {{ ref('fct_openalex_work_publication') }} fct
    INNER JOIN unlp_work_seen USING (work_hk)
    INNER JOIN {{ ref('dim_openalex_work') }} dim_work USING (work_hk)
    INNER JOIN {{ ref('dim_openalex_institution') }} dim_institution
        ON dim_institution.institution_hk = unlp_work_seen.institution_hk
)

SELECT * FROM base
