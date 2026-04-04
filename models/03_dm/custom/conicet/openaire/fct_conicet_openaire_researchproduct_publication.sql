{%- set conicet_rel_organization_id = 'openorgs____::40b9f835648a3e0d057d6917dd7e54d5' -%}

WITH conicet_extract AS (
    SELECT
        researchproduct_hk,
        MIN(load_datetime) AS conicet_first_extract_datetime,
        MAX(load_datetime) AS conicet_last_extract_datetime
    FROM {{ ref('fct_openaire_researchproduct_extraction_fby_relorgid') }}
    WHERE organization_ror = 'https://ror.org/03cqe8w59'
    GROUP BY researchproduct_hk
),

base AS (
    SELECT 
        fct.researchproduct_hk,
        fct.researchproduct_id,
        conicet_first_extract_datetime,
        conicet_last_extract_datetime,
        'https://ror.org/03cqe8w59' AS relOrganizationId,

        publication_date,
        embargo_end_date,
        type,
        main_title,
        language_code,
        language_label,
        publisher,

        best_access_right,
        best_access_right_uri,
        publicly_funded,
        is_green,
        is_in_diamond_journal,
        
        downloads,
        views,
        
        citation_class,
        citation_count,
        impulse,
        impulse_class,
        influence,
        influence_class,
        popularity,
        popularity_class,

        has_publication_date,

        has_arxiv,
        has_single_arxiv,
        arxiv_count,

        has_doi,
        has_single_doi,
        doi_count,

        has_handle,
        has_single_handle,
        handle_count,

        has_mag,
        has_single_mag,
        mag_count,

        has_pmb,
        has_single_pmb,
        pmb_count,

        has_pmc,
        has_single_pmc,
        pmc_count,

        has_pmid,
        has_single_pmid,
        pmid_count

    FROM {{ ref('fct_openaire_researchproduct_publication') }} fct
    INNER JOIN conicet_extract USING (researchproduct_hk)
    INNER JOIN {{ ref('brg_openaire_researchproduct_pid_stats') }} USING (researchproduct_hk)
)

SELECT * FROM base
-- WHERE has_arxiv = false AND has_doi = false AND has_handle = false AND has_mag = false AND has_pmb = false AND has_pmc = false AND has_pmid = false
