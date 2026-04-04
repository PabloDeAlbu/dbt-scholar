{{ config(materialized = 'table') }}

WITH base AS (
    SELECT
        record_hk,
        record_id,
        title,
        date_issued,
        repository_identifier,
        institution_ror,
        coar_type,
        coar_uri,
        coar_right,
        dc_identifier_uri,
        dc_relation_doi,
        doi_audit_string,
        has_doi,
        has_multiple_doi,
        doi_count_check,
        subject_area,
        subject_subarea,
        fos_code_level_1,
        mult_fos_flag
    FROM {{ ref('fct_conicet_oai_record_publication') }}
),

final AS (
    SELECT
        record_hk,
        record_id,
        'oai'::text AS source_system,
        title,
        date_issued AS publication_date,
        CASE
            WHEN date_issued IS NOT NULL THEN EXTRACT(YEAR FROM date_issued)::integer
        END AS publication_year,
        coar_type AS publication_type,
        coar_uri AS publication_type_uri,
        coar_right AS access_right,
        repository_identifier,
        institution_ror,
        dc_identifier_uri AS institutional_uri,
        dc_relation_doi AS doi,
        doi_audit_string,
        has_doi,
        has_multiple_doi,
        doi_count_check AS doi_count,
        subject_area,
        subject_subarea,
        fos_code_level_1 AS subject_code,
        mult_fos_flag AS has_multiple_subject_assignments
    FROM base
)

SELECT * FROM final
