{{ config(materialized = 'table') }}

{%- set conicet_repository_identifier = var('oai_repository_identifier') -%}
{%- set conicet_institution_ror = var('oai_institution_ror') -%}

WITH
-- ¿Qué atributos describen la última representación de cada record OAI de CONICET?
base as (
    SELECT
        fct.record_id,
        fct.title,
        fct.date_issued,
        fct.valid_date_issued,
        fct.extract_datetime,
        fct.repository_identifier,
        fct.institution_ror,
        fct.dc_type,
        fct.dc_type_count,
        fct.has_multiple_dc_type,
        fct.dc_right,
        fct.dc_right_count,
        fct.has_multiple_dc_right,
        fct.record_hk
    FROM {{ ref('fct_oai_record_publication') }} fct
    WHERE fct.repository_identifier = '{{ conicet_repository_identifier }}'
      AND fct.institution_ror = '{{ conicet_institution_ror }}'
),

-- ¿Qué condiciones de acceso COAR corresponden a los valores dc.right de cada record?
access_right_candidates AS (
    SELECT
        brg.record_hk,
        dim.access_right,
        dim.access_right_label_es,
        dim.access_right_uri
    FROM {{ ref('brg_oai_record_right') }} AS brg
    INNER JOIN base
        ON brg.record_hk = base.record_hk
    INNER JOIN {{ ref('dim_conicet_oai_record_accessright') }} AS dim
        ON brg.dc_right = dim.dc_right
),

-- ¿Qué condición de acceso se proyecta y qué multiplicidad presenta cada record?
resolved_access_right AS (
    SELECT
        record_hk,
        MIN(access_right) AS access_right,
        MIN(access_right_label_es) AS access_right_label_es,
        MIN(access_right_uri) AS access_right_uri,
        COUNT(DISTINCT access_right_uri) AS access_right_count,
        (COUNT(DISTINCT access_right_uri) > 1) AS has_multiple_access_right
    FROM access_right_candidates
    GROUP BY record_hk
),

-- ¿Qué representación analítica expone el hecho para cada record OAI de CONICET?
final AS (
    SELECT
        base.*,
        selected_resource_type.resource_type,
        selected_resource_type.resource_type_uri,
        selected_resource_type.resource_type_label_es,
        selected_resource_type.resource_type_source,
        selected_resource_type.resource_type_source_dc_types,
        COALESCE(selected_resource_type.resource_type_count, 0) AS resource_type_count,
        COALESCE(selected_resource_type.has_multiple_resource_types, false) AS has_multiple_resource_types,
        COALESCE(selected_resource_type.has_resource_type, false) AS has_resource_type,
        resolved_access_right.access_right,
        resolved_access_right.access_right_label_es,
        resolved_access_right.access_right_uri,
        COALESCE(resolved_access_right.access_right_count, 0) AS access_right_count,
        COALESCE(resolved_access_right.has_multiple_access_right, false) AS has_multiple_access_right,
        (resolved_access_right.access_right_uri IS NOT NULL) AS has_access_right,
        uri.dc_identifier_uri,
        doi.primary_doi as dc_relation_doi,
        COALESCE(doi.all_dois_concatenated, doi.primary_doi) as doi_audit_string,
        CASE WHEN doi.count_doi > 0 THEN true ELSE false END as has_doi,
        CASE WHEN doi.count_doi > 1 THEN true ELSE false END as has_multiple_doi,
        COALESCE(doi.count_doi, 0) as doi_count_check,
        fos.fos_label_level_1 as subject_area,
        fos.fos_label_level_2 as subject_subarea,
        fos.fos_code_level_1,
        COALESCE(fos.fos_assignment_count, 0) as fos_assignment_count,
        COALESCE(fos.has_multiple_fos_assignments, false) as mult_fos_flag
    FROM base
    LEFT JOIN {{ ref('int_conicet_oai_record_resource_type_selected') }} AS selected_resource_type
        USING (record_hk)
    LEFT JOIN resolved_access_right USING (record_hk)
    INNER JOIN {{ ref('brg_conicet_item_identifier_uri') }} uri USING (record_hk)
    LEFT JOIN {{ ref('brg_conicet_item_relation_doi') }} doi USING (record_hk)
    LEFT JOIN {{ ref('int_conicet_item_fos_selected') }} fos USING (record_hk)
)

SELECT * FROM final
