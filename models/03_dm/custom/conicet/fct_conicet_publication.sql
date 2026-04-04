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

access_right AS (
    SELECT
        brg.record_hk,
        MIN(seed.coar_label) AS access_right,
        MIN(seed.coar_uri) AS access_right_uri
    FROM {{ ref('brg_oai_record_right') }} brg
    INNER JOIN {{ ref('seed_coar_access_right') }} seed
        USING (dc_right)
    GROUP BY brg.record_hk
),

final AS (
    SELECT
        base.record_hk,
        base.record_id,
        'oai'::text AS source_system,
        base.title,
        base.date_issued AS publication_date,
        CASE
            WHEN base.date_issued IS NOT NULL THEN EXTRACT(YEAR FROM base.date_issued)::integer
        END AS publication_year,
        base.coar_type AS publication_type,
        base.coar_uri AS publication_type_uri,
        access_right.access_right,
        access_right.access_right_uri,
        base.repository_identifier,
        base.institution_ror,
        base.dc_identifier_uri AS institutional_uri,
        base.dc_relation_doi AS doi,
        base.doi_audit_string,
        base.has_doi,
        base.has_multiple_doi,
        base.doi_count_check AS doi_count,
        base.subject_area,
        base.subject_subarea,
        base.fos_code_level_1 AS subject_code,
        base.mult_fos_flag AS has_multiple_subject_assignments
    FROM base
    LEFT JOIN access_right USING (record_hk)
)

SELECT * FROM final
