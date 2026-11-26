-- ¿Cuántos identificadores distintos de cada esquema conserva cada work de OpenAlex?
WITH pid_count AS (
    SELECT
        work_hk,
        COUNT(DISTINCT pid_value) FILTER (WHERE pid_scheme = 'doi') AS doi_count,
        COUNT(DISTINCT pid_value) FILTER (WHERE pid_scheme = 'mag') AS mag_count,
        COUNT(DISTINCT pid_value) FILTER (WHERE pid_scheme = 'pmcid') AS pmcid_count,
        COUNT(DISTINCT pid_value) FILTER (WHERE pid_scheme = 'pmid') AS pmid_count
    FROM {{ ref('brg_openalex_work_pid') }}
    GROUP BY work_hk
),

-- ¿Qué PIDs pueden atribuirse sin ambigüedad a cada work de OpenAlex?
pid_unambiguous AS (
    SELECT DISTINCT
        relationship.work_hk,
        relationship.pid_scheme,
        relationship.pid_value
    FROM {{ ref('brg_openalex_work_pid') }} relationship
    INNER JOIN pid_count
        USING (work_hk)
    WHERE (relationship.pid_scheme = 'doi' AND pid_count.doi_count = 1)
       OR (relationship.pid_scheme = 'mag' AND pid_count.mag_count = 1)
       OR (relationship.pid_scheme = 'pmcid' AND pid_count.pmcid_count = 1)
       OR (relationship.pid_scheme = 'pmid' AND pid_count.pmid_count = 1)
),

-- ¿Qué DOI unívoco corresponde a cada work?
doi AS (
    SELECT work_hk, pid_value AS doi
    FROM pid_unambiguous
    WHERE pid_scheme = 'doi'
),

-- ¿Qué identificador MAG unívoco corresponde a cada work?
mag AS (
    SELECT work_hk, pid_value AS mag
    FROM pid_unambiguous
    WHERE pid_scheme = 'mag'
),

-- ¿Qué PMCID unívoco corresponde a cada work?
pmcid AS (
    SELECT work_hk, pid_value AS pmcid
    FROM pid_unambiguous
    WHERE pid_scheme = 'pmcid'
),

-- ¿Qué PMID unívoco corresponde a cada work?
pmid AS (
    SELECT work_hk, pid_value AS pmid
    FROM pid_unambiguous
    WHERE pid_scheme = 'pmid'
),

-- ¿Qué atributos de cobertura de PIDs expone cada work de OpenAlex?
final AS (
    SELECT
        work.work_hk,
        COALESCE(doi.doi, '-') AS doi,
        COALESCE(pid_count.doi_count, 0) AS doi_count,
        COALESCE(pid_count.doi_count, 0) > 0 AS has_doi,
        COALESCE(pid_count.doi_count, 0) = 1 AS has_single_doi,
        COALESCE(mag.mag, '-') AS mag,
        COALESCE(pid_count.mag_count, 0) AS mag_count,
        COALESCE(pid_count.mag_count, 0) > 0 AS has_mag,
        COALESCE(pid_count.mag_count, 0) = 1 AS has_single_mag,
        COALESCE(pmcid.pmcid, '-') AS pmcid,
        COALESCE(pid_count.pmcid_count, 0) AS pmcid_count,
        COALESCE(pid_count.pmcid_count, 0) > 0 AS has_pmcid,
        COALESCE(pid_count.pmcid_count, 0) = 1 AS has_single_pmcid,
        COALESCE(pmid.pmid, '-') AS pmid,
        COALESCE(pid_count.pmid_count, 0) AS pmid_count,
        COALESCE(pid_count.pmid_count, 0) > 0 AS has_pmid,
        COALESCE(pid_count.pmid_count, 0) = 1 AS has_single_pmid
    FROM {{ ref('hub_openalex_work') }} work
    LEFT JOIN pid_count
        USING (work_hk)
    LEFT JOIN doi
        USING (work_hk)
    LEFT JOIN mag
        USING (work_hk)
    LEFT JOIN pmcid
        USING (work_hk)
    LEFT JOIN pmid
        USING (work_hk)
)

SELECT *
FROM final
