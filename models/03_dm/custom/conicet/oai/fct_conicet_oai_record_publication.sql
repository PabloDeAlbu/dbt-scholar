{{ config(materialized = 'table') }}

{%- set conicet_repository_identifier = var('oai_repository_identifier') -%}
{%- set conicet_institution_ror = var('oai_institution_ror') -%}

WITH
base as (
    SELECT
        fct.record_id,
        fct.title,
        fct.date_issued,
        fct.repository_identifier,
        fct.institution_ror,
        fct.dc_type,
        fct.dc_type_count,
        fct.has_multiple_dc_type,
        fct.record_hk
    FROM {{ ref('fct_oai_record_publication') }} fct
    WHERE fct.repository_identifier = '{{ conicet_repository_identifier }}'
      AND fct.institution_ror = '{{ conicet_institution_ror }}'
),

final AS (
    SELECT
        base.*,
        uri.dc_identifier_uri,
        doi.primary_doi as dc_relation_doi,
        COALESCE(doi.all_dois_concatenated, doi.primary_doi) as doi_audit_string,
        CASE WHEN doi.count_doi > 0 THEN true ELSE false END as has_doi,
        CASE WHEN doi.count_doi > 1 THEN true ELSE false END as has_multiple_doi,
        COALESCE(doi.count_doi, 0) as doi_count_check,
        fos.fos_label_level_1 as subject_area,
        fos.fos_label_level_2 as subject_subarea,
        fos.fos_code_level_1,
        COALESCE(fos.has_multiple_fos_assignments, false) as mult_fos_flag
    FROM base
    INNER JOIN {{ ref('brg_conicet_item_identifier_uri') }} uri USING (record_hk)
    LEFT JOIN {{ ref('brg_conicet_item_relation_doi') }} doi USING (record_hk)
    LEFT JOIN {{ ref('brg_conicet_item_fos_flattened') }} fos USING (record_hk)
)

SELECT * FROM final
