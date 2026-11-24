{{ config(materialized = 'table') }}

-- ¿Cuál es la observación más reciente de cada researchproduct de OpenAIRE?
WITH latest_rp AS (
    SELECT *
    FROM {{ ref('latest_sat_openaire_researchproduct') }}
),

-- ¿Cómo se describe el tipo principal de cada researchproduct?
dim_type AS (
    SELECT *
    FROM {{ ref('dim_openaire_researchproduct_type') }}
),

-- ¿Cuántos PIDs distintos de cada esquema fueron observados para cada researchproduct?
pid_profile AS (
    SELECT
        researchproduct_hk,
        COUNT(DISTINCT pid_hk) FILTER (WHERE scheme = 'doi') AS doi_count,
        COUNT(DISTINCT pid_hk) FILTER (WHERE scheme = 'handle') AS handle_count,
        COUNT(DISTINCT pid_hk) FILTER (WHERE scheme = 'arXiv') AS arxiv_count,
        COUNT(DISTINCT pid_hk) FILTER (WHERE scheme = 'mag_id') AS mag_count,
        COUNT(DISTINCT pid_hk) FILTER (WHERE scheme = 'pmb') AS pmb_count,
        COUNT(DISTINCT pid_hk) FILTER (WHERE scheme = 'pmc') AS pmc_count,
        COUNT(DISTINCT pid_hk) FILTER (WHERE scheme = 'pmid') AS pmid_count
    FROM {{ ref('brg_openaire_researchproduct_pid') }}
    GROUP BY researchproduct_hk
),

-- ¿Qué atributos, métricas y cobertura de PIDs describen el hecho de publicación
-- de cada researchproduct de OpenAIRE?
final AS (
    SELECT
        dim_rp.researchproduct_id,

        sat_rp.load_datetime,
        sat_rp.publication_date,
        sat_rp.embargo_end_date,
        dim_type.researchproduct_type AS type,
        dim_type.researchproduct_type_uri AS type_uri,
        sat_rp.main_title,
        sat_rp.language_code,
        sat_rp.language_label,
        sat_rp.publisher,

        sat_rp.best_access_right,
        sat_rp.best_access_right_uri,
        sat_rp.publicly_funded,
        sat_rp.is_green,
        sat_rp.is_in_diamond_journal,

        COALESCE(sat_rp.downloads, 0) AS downloads,
        COALESCE(sat_rp.views, 0) AS views,
        sat_rp.citation_class,
        sat_rp.citation_count,
        sat_rp.impulse,
        sat_rp.impulse_class,
        sat_rp.influence,
        sat_rp.influence_class,
        sat_rp.popularity,
        sat_rp.popularity_class,

        sat_rp.publication_date IS NOT NULL AS has_publication_date,

        COALESCE(pid.arxiv_count, 0) AS arxiv_count,
        COALESCE(pid.arxiv_count, 0) > 0 AS has_arxiv,
        COALESCE(pid.arxiv_count, 0) = 1 AS has_single_arxiv,

        COALESCE(pid.doi_count, 0) AS doi_count,
        COALESCE(pid.doi_count, 0) > 0 AS has_doi,
        COALESCE(pid.doi_count, 0) = 1 AS has_single_doi,

        COALESCE(pid.handle_count, 0) AS handle_count,
        COALESCE(pid.handle_count, 0) > 0 AS has_handle,
        COALESCE(pid.handle_count, 0) = 1 AS has_single_handle,

        COALESCE(pid.mag_count, 0) AS mag_count,
        COALESCE(pid.mag_count, 0) > 0 AS has_mag,
        COALESCE(pid.mag_count, 0) = 1 AS has_single_mag,

        COALESCE(pid.pmb_count, 0) AS pmb_count,
        COALESCE(pid.pmb_count, 0) > 0 AS has_pmb,
        COALESCE(pid.pmb_count, 0) = 1 AS has_single_pmb,

        COALESCE(pid.pmc_count, 0) AS pmc_count,
        COALESCE(pid.pmc_count, 0) > 0 AS has_pmc,
        COALESCE(pid.pmc_count, 0) = 1 AS has_single_pmc,

        COALESCE(pid.pmid_count, 0) AS pmid_count,
        COALESCE(pid.pmid_count, 0) > 0 AS has_pmid,
        COALESCE(pid.pmid_count, 0) = 1 AS has_single_pmid,

        sat_rp.source,
        dim_rp.researchproduct_hk
    FROM {{ ref('dim_openaire_researchproduct') }} AS dim_rp
    INNER JOIN latest_rp AS sat_rp
        USING (researchproduct_hk)
    LEFT JOIN dim_type
        ON sat_rp.type = dim_type.researchproduct_type
    LEFT JOIN pid_profile AS pid
        USING (researchproduct_hk)
)

SELECT *
FROM final
