WITH base AS (
    SELECT 
        work.work_hk,
        work.work_id,
        hub_doi.doi,
        hub_mag.mag,
        hub_pmcid.pmcid,
        hub_pmid.pmid,
        CASE WHEN hub_doi.doi <> '-' THEN TRUE ELSE FALSE END AS has_doi,
        CASE WHEN hub_mag.mag <> '-' THEN TRUE ELSE FALSE END AS has_mag,
        CASE WHEN hub_pmcid.pmcid <> '-' THEN TRUE ELSE FALSE END AS has_pmcid,
        CASE WHEN hub_pmid.pmid <> '-' THEN TRUE ELSE FALSE END AS has_pmid
    FROM {{ref('openalex_work')}} work 
    LEFT JOIN {{ref('link_openalex_work_doi')}} link_work_doi USING (work_hk)
    LEFT JOIN {{ref('hub_openalex_doi')}} hub_doi USING (doi_hk)
    LEFT JOIN {{ref('link_openalex_work_mag')}} link_work_mag USING (work_hk)
    LEFT JOIN {{ref('hub_openalex_mag')}} hub_mag USING (mag_hk)
    LEFT JOIN {{ref('link_openalex_work_pmcid')}} link_work_pmcid USING (work_hk)
    LEFT JOIN {{ref('hub_openalex_pmcid')}} hub_pmcid USING (pmcid_hk)
    LEFT JOIN {{ref('link_openalex_work_pmid')}} link_work_pmid USING (work_hk)
    LEFT JOIN {{ref('hub_openalex_pmid')}} hub_pmid USING (pmid_hk)
)

SELECT * FROM base