WITH latest_sat_work AS (
    SELECT *
    FROM {{ ref('latest_sat_openalex_work') }}
),

author AS (
    SELECT
        brg.work_hk,
        STRING_AGG(
            DISTINCT dim_author.display_name::text,
            '|' ORDER BY dim_author.display_name::text
        ) AS authors
    FROM {{ ref('brg_openalex_work_author') }} brg
    INNER JOIN {{ ref('dim_openalex_author') }} dim_author USING (author_hk)
    GROUP BY brg.work_hk
),

topic AS (
    SELECT
        brg.work_hk,
        STRING_AGG(
            DISTINCT dim_topic.display_name::text,
            '|' ORDER BY dim_topic.display_name::text
        ) AS topics
    FROM {{ ref('brg_openalex_work_topic') }} brg
    INNER JOIN {{ ref('dim_openalex_topic') }} dim_topic USING (topic_hk)
    GROUP BY brg.work_hk
),

pid AS (
    SELECT
        work_hk,
        doi,
        mag,
        pmcid,
        pmid,
        has_doi,
        has_mag,
        has_pmcid,
        has_pmid
    FROM {{ ref('dim_openalex_work_pid') }}
),

primary_location AS (
    SELECT
        work_hk,
        source_display_name,
        source_issn_l,
        license
    FROM {{ ref('brg_openalex_work_primarylocation') }}
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
        primary_location.source_display_name,
        primary_location.source_issn_l AS issn,
        primary_location.license,
        sat_work.any_repository_has_fulltext,
        sat_work.is_oa,
        sat_work.oa_status,
        sat_work.oa_url,
        NULLIF(sat_work.biblio_first_page, '!UNKNOWN') AS biblio_first_page,
        sat_work.biblio_issue,
        NULLIF(sat_work.biblio_last_page, '!UNKNOWN') AS biblio_last_page,
        sat_work.biblio_volume,
        CASE
            WHEN NULLIF(sat_work.biblio_first_page, '!UNKNOWN') IS NOT NULL
             AND NULLIF(sat_work.biblio_last_page, '!UNKNOWN') IS NOT NULL
                THEN NULLIF(sat_work.biblio_first_page, '!UNKNOWN') || '-' || NULLIF(sat_work.biblio_last_page, '!UNKNOWN')
            WHEN NULLIF(sat_work.biblio_first_page, '!UNKNOWN') IS NOT NULL
                THEN NULLIF(sat_work.biblio_first_page, '!UNKNOWN')
            WHEN NULLIF(sat_work.biblio_last_page, '!UNKNOWN') IS NOT NULL
                THEN NULLIF(sat_work.biblio_last_page, '!UNKNOWN')
        END AS pages,
        author.authors,
        topic.topics,
        NULLIF(pid.doi, '-') AS doi,
        NULLIF(pid.mag, '-') AS mag,
        NULLIF(pid.pmcid, '-') AS pmcid,
        NULLIF(pid.pmid, '-') AS pmid,
        COALESCE(pid.has_doi, FALSE) AS has_doi,
        COALESCE(pid.has_mag, FALSE) AS has_mag,
        COALESCE(pid.has_pmcid, FALSE) AS has_pmcid,
        COALESCE(pid.has_pmid, FALSE) AS has_pmid,
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
    LEFT JOIN author USING (work_hk)
    LEFT JOIN topic USING (work_hk)
    LEFT JOIN pid USING (work_hk)
    LEFT JOIN primary_location USING (work_hk)
)

SELECT * FROM final
