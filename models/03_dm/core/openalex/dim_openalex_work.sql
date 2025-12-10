WITH sat_work AS {{ latest_satellite(ref('sat_openalex_work'), 'work_hk') }},

work AS (
    SELECT
        hub_work.work_id,
        hub_work.work_hk,
        sat_work.title,
        sat_work.type,
        CASE
            WHEN sat_work.publication_year IS NOT NULL THEN TO_DATE(sat_work.publication_year::text, 'YYYY')
        END AS publication_year,
        sat_work.publication_date,
        sat_work.has_fulltext,
        sat_work.fulltext_origin,
        sat_work.is_retracted,
        sat_work.is_paratext,
        sat_work.any_repository_has_fulltext,
        sat_work.is_oa,
        sat_work.oa_status,
        sat_work.oa_url
    FROM {{ref('hub_openalex_work')}} hub_work
    INNER JOIN sat_work USING (work_hk)
),

work_pid AS (
    SELECT
        hub_work.work_hk,
        MAX(REPLACE(hub_doi.doi, 'https://doi.org/', '')) AS doi,
        MAX(hub_mag.mag) AS mag,
        MAX(hub_pmcid.pmcid) AS pmcid,
        MAX(hub_pmid.pmid) AS pmid
    FROM {{ref('hub_openalex_work')}} hub_work
    LEFT JOIN {{ref('link_openalex_work_doi')}} link_work_doi ON link_work_doi.work_hk = hub_work.work_hk
    LEFT JOIN {{ref('hub_openalex_doi')}} hub_doi ON hub_doi.doi_hk = link_work_doi.doi_hk
    LEFT JOIN {{ref('link_openalex_work_mag')}} link_work_mag ON link_work_mag.work_hk = hub_work.work_hk
    LEFT JOIN {{ref('hub_openalex_mag')}} hub_mag ON hub_mag.mag_hk = link_work_mag.mag_hk
    LEFT JOIN {{ref('link_openalex_work_pmcid')}} link_work_pmcid ON link_work_pmcid.work_hk = hub_work.work_hk
    LEFT JOIN {{ref('hub_openalex_pmcid')}} hub_pmcid ON hub_pmcid.pmcid_hk = link_work_pmcid.pmcid_hk
    LEFT JOIN {{ref('link_openalex_work_pmid')}} link_work_pmid ON link_work_pmid.work_hk = hub_work.work_hk
    LEFT JOIN {{ref('hub_openalex_pmid')}} hub_pmid ON hub_pmid.pmid_hk = link_work_pmid.pmid_hk
    GROUP BY hub_work.work_hk
),

dim AS (
    SELECT
        work.work_id,
        work.work_hk,
        work.title,
        work.type,
        work.publication_year,
        work.publication_date,
        work.has_fulltext,
        work.fulltext_origin,
        work.is_retracted,
        work.is_paratext,
        work.any_repository_has_fulltext,
        work.is_oa,
        work.oa_status,
        work.oa_url,
        COALESCE(NULLIF(work_pid.doi, ''), '-') AS doi,
        COALESCE(work_pid.mag, '-') AS mag,
        COALESCE(work_pid.pmcid, '-') AS pmcid,
        COALESCE(work_pid.pmid, '-') AS pmid,
        CASE WHEN COALESCE(NULLIF(work_pid.doi, ''), '-') <> '-' THEN TRUE ELSE FALSE END AS has_doi,
        CASE WHEN COALESCE(work_pid.mag, '-') <> '-' THEN TRUE ELSE FALSE END AS has_mag,
        CASE WHEN COALESCE(work_pid.pmcid, '-') <> '-' THEN TRUE ELSE FALSE END AS has_pmcid,
        CASE WHEN COALESCE(work_pid.pmid, '-') <> '-' THEN TRUE ELSE FALSE END AS has_pmid
    FROM work
    LEFT JOIN work_pid USING (work_hk)
)

SELECT * FROM dim
