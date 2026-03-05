{{ config(materialized = 'table') }}

WITH base AS (
    SELECT
        rp.researchproduct_id,
        rp.researchproduct_hk,

        SUM(CASE WHEN brg.scheme = 'doi' THEN 1 ELSE 0 END) AS doi_count,
        SUM(CASE WHEN brg.scheme = 'handle' THEN 1 ELSE 0 END) AS handle_count,
        SUM(CASE WHEN brg.scheme = 'arXiv' THEN 1 ELSE 0 END) AS arxiv_count,
        SUM(CASE WHEN brg.scheme = 'mag_id' THEN 1 ELSE 0 END) AS mag_count,
        SUM(CASE WHEN brg.scheme = 'pmb' THEN 1 ELSE 0 END) AS pmb_count,
        SUM(CASE WHEN brg.scheme = 'pmc' THEN 1 ELSE 0 END) AS pmc_count,
        SUM(CASE WHEN brg.scheme = 'pmid' THEN 1 ELSE 0 END) AS pmid_count
    FROM {{ ref('dim_openaire_researchproduct') }} rp
    LEFT JOIN {{ ref('brg_openaire_researchproduct_pid') }} brg USING (researchproduct_hk)
    GROUP BY 1, 2
),

final AS (
    SELECT
        researchproduct_id,
        researchproduct_hk,

        COALESCE(doi_count, 0) AS doi_count,
        (COALESCE(doi_count, 0) > 0) AS has_doi,
        (COALESCE(doi_count, 0) = 1) AS has_single_doi,

        COALESCE(handle_count, 0) AS handle_count,
        (COALESCE(handle_count, 0) > 0) AS has_handle,
        (COALESCE(handle_count, 0) = 1) AS has_single_handle,

        COALESCE(arxiv_count, 0) AS arxiv_count,
        (COALESCE(arxiv_count, 0) > 0) AS has_arxiv,
        (COALESCE(arxiv_count, 0) = 1) AS has_single_arxiv,

        COALESCE(mag_count, 0) AS mag_count,
        (COALESCE(mag_count, 0) > 0) AS has_mag,
        (COALESCE(mag_count, 0) = 1) AS has_single_mag,

        COALESCE(pmb_count, 0) AS pmb_count,
        (COALESCE(pmb_count, 0) > 0) AS has_pmb,
        (COALESCE(pmb_count, 0) = 1) AS has_single_pmb,

        COALESCE(pmc_count, 0) AS pmc_count,
        (COALESCE(pmc_count, 0) > 0) AS has_pmc,
        (COALESCE(pmc_count, 0) = 1) AS has_single_pmc,

        COALESCE(pmid_count, 0) AS pmid_count,
        (COALESCE(pmid_count, 0) > 0) AS has_pmid,
        (COALESCE(pmid_count, 0) = 1) AS has_single_pmid
    FROM base
)

SELECT * FROM final

