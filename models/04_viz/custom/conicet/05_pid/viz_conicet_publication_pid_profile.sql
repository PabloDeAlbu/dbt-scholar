WITH base AS (
    SELECT
        publication.record_hk,
        publication.record_id,
        publication.title,
        publication.publication_year,
        publication.publication_type,
        publication.subject_area,
        publication.institutional_uri,
        publication.doi,
        publication.has_doi,
        publication.matched_by_unique_doi,
        COALESCE(openaire.has_doi, false) AS has_openaire_doi,
        COALESCE(openaire.doi_count, 0) AS openaire_doi_count,
        COALESCE(openaire.has_handle, false) AS has_openaire_handle,
        COALESCE(openaire.handle_count, 0) AS openaire_handle_count,
        COALESCE(openaire.has_arxiv, false) AS has_openaire_arxiv,
        COALESCE(openaire.arxiv_count, 0) AS openaire_arxiv_count,
        COALESCE(openaire.has_pmid, false) AS has_openaire_pmid,
        COALESCE(openaire.pmid_count, 0) AS openaire_pmid_count,
        COALESCE(openaire.has_pmc, false) AS has_openaire_pmc,
        COALESCE(openaire.pmc_count, 0) AS openaire_pmc_count,
        COALESCE(openaire.has_pmb, false) AS has_openaire_pmb,
        COALESCE(openaire.pmb_count, 0) AS openaire_pmb_count,
        COALESCE(openaire.has_mag, false) AS has_openaire_mag,
        COALESCE(openaire.mag_count, 0) AS openaire_mag_count
    FROM {{ ref('fct_conicet_publication') }} AS publication
    LEFT JOIN {{ ref('fct_conicet_openaire_researchproduct_publication') }} AS openaire
        ON publication.openaire_researchproduct_hk = openaire.researchproduct_hk
),

final AS (
    SELECT
        record_hk,
        record_id,
        title,
        publication_year,
        publication_type,
        subject_area,
        institutional_uri,
        doi,
        has_doi,
        matched_by_unique_doi,
        (institutional_uri IS NOT NULL AND TRIM(institutional_uri) <> '') AS has_repo_handle,
        has_openaire_doi,
        openaire_doi_count,
        has_openaire_handle,
        openaire_handle_count,
        has_openaire_arxiv,
        openaire_arxiv_count,
        has_openaire_pmid,
        openaire_pmid_count,
        has_openaire_pmc,
        openaire_pmc_count,
        has_openaire_pmb,
        openaire_pmb_count,
        has_openaire_mag,
        openaire_mag_count,
        CASE
            WHEN has_doi THEN 'Con DOI'
            ELSE 'Sin DOI'
        END AS has_doi_label
    FROM base
)

SELECT * FROM final
