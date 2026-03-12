WITH base AS (
    SELECT
        fct_unlp.researchproduct_hk,
        fct_unlp.researchproduct_id,
        fct_unlp.organization_ror,
        fct_unlp.unlp_first_extract_datetime,
        fct_unlp.unlp_last_extract_datetime,
        fct_unlp.publication_date,
        fct_unlp.type,
        fct_unlp.main_title,
        fct_unlp.publisher,
        fct_unlp.best_access_right,
        fct_unlp.has_doi,
        fct_unlp.doi_count,
        brg_sdg.subject_hk,
        brg_sdg.subject_value AS sdg
    FROM {{ ref('fct_unlp_openaire_researchproduct') }} fct_unlp
    INNER JOIN {{ ref('brg_openaire_researchproduct_subject_sdg') }} brg_sdg USING (researchproduct_hk)
)

SELECT * FROM base
