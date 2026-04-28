WITH work_pid AS (
    SELECT
        hub_work.work_hk,
        MAX(REPLACE(hub_doi.doi, 'https://doi.org/', '')) AS doi,
        MAX(hub_mag.mag) AS mag,
        MAX(hub_pmcid.pmcid) AS pmcid,
        MAX(hub_pmid.pmid) AS pmid
    FROM {{ ref('hub_openalex_work') }} hub_work
    LEFT JOIN {{ ref('link_openalex_work_doi') }} link_work_doi
        ON link_work_doi.work_hk = hub_work.work_hk
    LEFT JOIN {{ ref('hub_openalex_doi') }} hub_doi
        ON hub_doi.doi_hk = link_work_doi.doi_hk
    LEFT JOIN {{ ref('link_openalex_work_mag') }} link_work_mag
        ON link_work_mag.work_hk = hub_work.work_hk
    LEFT JOIN {{ ref('hub_openalex_mag') }} hub_mag
        ON hub_mag.mag_hk = link_work_mag.mag_hk
    LEFT JOIN {{ ref('link_openalex_work_pmcid') }} link_work_pmcid
        ON link_work_pmcid.work_hk = hub_work.work_hk
    LEFT JOIN {{ ref('hub_openalex_pmcid') }} hub_pmcid
        ON hub_pmcid.pmcid_hk = link_work_pmcid.pmcid_hk
    LEFT JOIN {{ ref('link_openalex_work_pmid') }} link_work_pmid
        ON link_work_pmid.work_hk = hub_work.work_hk
    LEFT JOIN {{ ref('hub_openalex_pmid') }} hub_pmid
        ON hub_pmid.pmid_hk = link_work_pmid.pmid_hk
    GROUP BY hub_work.work_hk
),

dim AS (
    SELECT
        work_hk,
        COALESCE(NULLIF(doi, ''), '-') AS doi,
        COALESCE(mag, '-') AS mag,
        COALESCE(pmcid, '-') AS pmcid,
        COALESCE(pmid, '-') AS pmid,
        CASE WHEN COALESCE(NULLIF(doi, ''), '-') <> '-' THEN TRUE ELSE FALSE END AS has_doi,
        CASE WHEN COALESCE(mag, '-') <> '-' THEN TRUE ELSE FALSE END AS has_mag,
        CASE WHEN COALESCE(pmcid, '-') <> '-' THEN TRUE ELSE FALSE END AS has_pmcid,
        CASE WHEN COALESCE(pmid, '-') <> '-' THEN TRUE ELSE FALSE END AS has_pmid
    FROM work_pid
)

SELECT * FROM dim
