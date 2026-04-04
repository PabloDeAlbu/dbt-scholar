{{ config(materialized = 'table') }}

WITH base AS (
    SELECT
        record_hk,
        record_id,
        title,
        date_issued,
        repository_identifier,
        institution_ror,
        dc_type,
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

dim_publication_type AS (
    SELECT
        seed.record_type AS dc_type,
        dim.resource_type_label AS publication_type,
        dim.resource_type_uri AS publication_type_uri
    FROM {{ ref('seed_coar_resource_types2conicet_oai_dc_types') }} seed
    INNER JOIN {{ ref('dim_resource_type') }} dim
        ON seed.coar_uri = dim.resource_type_uri
    WHERE seed.coar_uri != '#N/A'
),

dim_access_right AS (
    SELECT
        brg.record_hk,
        MIN(dim.access_right_label) AS access_right,
        MIN(dim.access_right_uri) AS access_right_uri
    FROM {{ ref('brg_oai_record_right') }} brg
    INNER JOIN {{ ref('dim_access_right') }} dim
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
        dim_publication_type.publication_type,
        dim_publication_type.publication_type_uri,
        dim_access_right.access_right,
        dim_access_right.access_right_uri,
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
    LEFT JOIN dim_publication_type
        ON base.dc_type = dim_publication_type.dc_type
    LEFT JOIN dim_access_right USING (record_hk)
)

SELECT * FROM final
