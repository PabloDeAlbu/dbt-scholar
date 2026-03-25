{%- set cic_ror = 'https://ror.org/02s7sax82' -%}

WITH cic_extract AS (
    SELECT
        work_hk,
        MIN(extract_datetime) AS cic_first_extract_datetime,
        MAX(extract_datetime) AS cic_last_extract_datetime
    FROM {{ ref('fct_openalex_work_extraction') }}
    WHERE _filter_param = 'institutions.ror'
      AND _filter_value = '{{ cic_ror }}'
    GROUP BY work_hk
),

cic_institution AS (
    SELECT
        institution_hk,
        institution_id,
        institution_display_name,
        ror AS institution_ror
    FROM {{ ref('dim_openalex_institution') }}
    WHERE ror = '{{ cic_ror }}'
),

base AS (
    SELECT
        fct.work_hk,
        fct.work_id,
        cic_institution.institution_hk,
        cic_institution.institution_id,
        cic_institution.institution_display_name,
        cic_institution.institution_ror,
        cic_extract.cic_first_extract_datetime,
        cic_extract.cic_last_extract_datetime,

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
    INNER JOIN cic_extract USING (work_hk)
    INNER JOIN {{ ref('dim_openalex_work') }} dim_work USING (work_hk)
    CROSS JOIN cic_institution
)

SELECT * FROM base
