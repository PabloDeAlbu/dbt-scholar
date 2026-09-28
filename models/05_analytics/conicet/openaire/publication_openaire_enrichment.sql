{{ config(materialized = 'view') }}

WITH base AS (
    SELECT
        record_hk,
        record_id,
        title,
        CASE WHEN valid_date_issued THEN date_issued END AS publication_date,
        publication_type_label_es AS publication_type,
        access_right_label_es AS access_right,
        dc_identifier_uri AS institutional_uri,
        dc_relation_doi AS doi,
        subject_area,
        subject_subarea
    FROM {{ ref('fct_conicet_oai_record_publication') }}
),

unique_match AS (
    SELECT
        record_hk,
        original_id AS openaire_original_id,
        researchproduct_hk,
        researchproduct_id
    FROM {{ ref('brg_conicet_publication_originalid') }}
    WHERE is_unique_match
),

openaire AS (
    SELECT
        researchproduct_hk,
        conicet_first_extract_datetime AS openaire_first_extract_datetime,
        conicet_last_extract_datetime AS openaire_last_extract_datetime,
        conicet_first_load_datetime AS openaire_first_load_datetime,
        conicet_last_load_datetime AS openaire_last_load_datetime,
        best_access_right AS openaire_access_right,
        best_access_right_uri AS openaire_access_right_uri,
        citation_class AS openaire_citation_class,
        citation_count AS openaire_citation_count
    FROM {{ ref('fct_conicet_openaire_researchproduct_publication') }}
),

final AS (
    SELECT
        base.record_id,
        base.title,
        base.publication_date,
        base.publication_type,
        base.access_right,
        base.institutional_uri,
        base.doi,
        base.subject_area,
        base.subject_subarea,
        (unique_match.researchproduct_hk IS NOT NULL) AS matched_by_unique_original_id,
        unique_match.openaire_original_id,
        unique_match.researchproduct_id AS openaire_researchproduct_id,
        openaire.openaire_first_extract_datetime,
        openaire.openaire_last_extract_datetime,
        openaire.openaire_first_load_datetime,
        openaire.openaire_last_load_datetime,
        openaire.openaire_access_right,
        openaire.openaire_access_right_uri,
        openaire.openaire_citation_class,
        openaire.openaire_citation_count
    FROM base
    LEFT JOIN unique_match USING (record_hk)
    LEFT JOIN openaire USING (researchproduct_hk)
)

SELECT *
FROM final
