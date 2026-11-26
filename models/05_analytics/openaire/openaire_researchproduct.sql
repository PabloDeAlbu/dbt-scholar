-- ¿Qué atributos y coberturas de identificadores expone cada researchproduct de OpenAIRE?
WITH final AS (
    SELECT 
        fct.researchproduct_id,

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

    FROM {{ ref('fct_openaire_researchproduct_publication') }} AS fct
)

SELECT *
FROM final
