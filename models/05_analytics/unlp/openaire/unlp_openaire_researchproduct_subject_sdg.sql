-- ¿Qué productos de investigación atribuidos a la UNLP informa OpenAIRE?
WITH unlp_researchproduct AS (
    SELECT
        researchproduct_hk,
        researchproduct_id,
        organization_ror,
        unlp_first_extract_datetime,
        unlp_last_extract_datetime,
        publication_date,
        type,
        main_title,
        publisher,
        best_access_right,
        has_doi,
        doi_count
    FROM {{ ref('fct_unlp_openaire_researchproduct') }}
),

-- ¿Qué Objetivos de Desarrollo Sostenible tiene asignados cada producto?
researchproduct_sdg AS (
    SELECT
        researchproduct_hk,
        subject_hk,
        subject_value AS sdg
    FROM {{ ref('brg_openaire_researchproduct_subject_sdg') }}
),

-- ¿Qué contrato analítico expone la producción UNLP a grano producto por ODS?
final AS (
    SELECT
        researchproduct.researchproduct_hk,
        researchproduct.researchproduct_id,
        researchproduct.organization_ror,
        researchproduct.unlp_first_extract_datetime,
        researchproduct.unlp_last_extract_datetime,
        researchproduct.publication_date,
        researchproduct.type,
        researchproduct.main_title,
        researchproduct.publisher,
        researchproduct.best_access_right,
        researchproduct.has_doi,
        researchproduct.doi_count,
        sdg.subject_hk,
        sdg.sdg
    FROM unlp_researchproduct AS researchproduct
    INNER JOIN researchproduct_sdg AS sdg
        USING (researchproduct_hk)
)

SELECT *
FROM final
