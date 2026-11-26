{{ config(materialized='table') }}

-- ¿Qué DOI están asociados con cada work de OpenAlex?
WITH doi AS (
    SELECT
        work_doi_hk AS work_pid_hk,
        work_hk,
        doi_hk AS pid_hk,
        'doi'::text AS pid_scheme,
        doi_raw::text AS pid_raw_value,
        doi::text AS pid_value
    FROM {{ ref('brg_openalex_work_doi') }}
),

-- ¿Qué identificadores MAG están asociados con cada work de OpenAlex?
mag AS (
    SELECT
        relationship.work_mag_hk AS work_pid_hk,
        relationship.work_hk,
        relationship.mag_hk AS pid_hk,
        'mag'::text AS pid_scheme,
        pid.mag::text AS pid_raw_value,
        pid.mag::text AS pid_value
    FROM {{ ref('link_openalex_work_mag') }} relationship
    INNER JOIN {{ ref('hub_openalex_mag') }} pid
        USING (mag_hk)
    WHERE pid.mag NOT IN ('!UNKNOWN', '-')
      AND pid.mag != ''
),

-- ¿Qué PMCID están asociados con cada work de OpenAlex?
pmcid AS (
    SELECT
        relationship.work_pmcid_hk AS work_pid_hk,
        relationship.work_hk,
        relationship.pmcid_hk AS pid_hk,
        'pmcid'::text AS pid_scheme,
        pid.pmcid::text AS pid_raw_value,
        pid.pmcid::text AS pid_value
    FROM {{ ref('link_openalex_work_pmcid') }} relationship
    INNER JOIN {{ ref('hub_openalex_pmcid') }} pid
        USING (pmcid_hk)
    WHERE pid.pmcid NOT IN ('!UNKNOWN', '-')
      AND pid.pmcid != ''
),

-- ¿Qué PMID están asociados con cada work de OpenAlex?
pmid AS (
    SELECT
        relationship.work_pmid_hk AS work_pid_hk,
        relationship.work_hk,
        relationship.pmid_hk AS pid_hk,
        'pmid'::text AS pid_scheme,
        pid.pmid::text AS pid_raw_value,
        pid.pmid::text AS pid_value
    FROM {{ ref('link_openalex_work_pmid') }} relationship
    INNER JOIN {{ ref('hub_openalex_pmid') }} pid
        USING (pmid_hk)
    WHERE pid.pmid NOT IN ('!UNKNOWN', '-')
      AND pid.pmid != ''
),

-- ¿Qué PIDs y esquemas conserva cada work de OpenAlex?
final AS (
    SELECT * FROM doi
    UNION ALL
    SELECT * FROM mag
    UNION ALL
    SELECT * FROM pmcid
    UNION ALL
    SELECT * FROM pmid
)

SELECT *
FROM final
