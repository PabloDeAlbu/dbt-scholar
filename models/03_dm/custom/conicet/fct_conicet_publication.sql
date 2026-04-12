{{ config(materialized = 'table') }}

WITH base AS (
    SELECT
        record_hk,
        record_id,
        title,
        date_issued,
        valid_date_issued,
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

record_dc_type AS (
    SELECT DISTINCT
        base.record_hk,
        NULLIF(dc_type_value, '') AS dc_type
    FROM base
    CROSS JOIN LATERAL REGEXP_SPLIT_TO_TABLE(COALESCE(base.dc_type, ''), '[|]') AS dc_type_value
    WHERE NULLIF(dc_type_value, '') IS NOT NULL
),

dim_publication_type AS (
    SELECT
        record_dc_type.record_hk,
        MIN(dim.resource_type_label) AS publication_type,
        MIN(dim.resource_type_uri) AS publication_type_uri,
        COUNT(DISTINCT dim.resource_type_uri) AS publication_type_count,
        (COUNT(DISTINCT dim.resource_type_uri) > 1) AS has_multiple_publication_type
    FROM record_dc_type
    INNER JOIN {{ ref('seed_coar_resource_types2conicet_oai_dc_types') }} seed
        ON record_dc_type.dc_type = seed.record_type
    INNER JOIN {{ ref('dim_resource_type') }} dim
        ON seed.coar_uri = dim.resource_type_uri
    WHERE seed.coar_uri != '#N/A'
    GROUP BY record_dc_type.record_hk
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

openaire_originalid AS (
    SELECT
        publication.researchproduct_hk,
        publication.researchproduct_id,
        original.original_id
    FROM {{ ref('fct_conicet_openaire_researchproduct_publication') }} AS publication
    INNER JOIN {{ ref('brg_openaire_researchproduct_originalid') }} AS bridge
        USING (researchproduct_hk)
    INNER JOIN {{ ref('stg_openaire_researchproduct_originalid') }} AS original
        ON original.researchproduct_hk = bridge.researchproduct_hk
       AND original.original_hk = bridge.original_hk
    WHERE original.original_id LIKE 'oai:ri.conicet.gov.ar:%'
),

originalid_match_count AS (
    SELECT
        original_id,
        COUNT(DISTINCT researchproduct_hk) AS openaire_researchproduct_count
    FROM openaire_originalid
    GROUP BY original_id
),

unique_originalid_match AS (
    SELECT
        base.record_hk,
        openaire_originalid.original_id AS openaire_original_id,
        openaire_originalid.researchproduct_hk,
        openaire_originalid.researchproduct_id
    FROM base
    INNER JOIN openaire_originalid
        ON openaire_originalid.original_id = base.record_id
    INNER JOIN originalid_match_count
        ON originalid_match_count.original_id = openaire_originalid.original_id
    WHERE originalid_match_count.openaire_researchproduct_count = 1
),

openaire_enrichment AS (
    SELECT
        match.record_hk,
        match.openaire_original_id,
        match.researchproduct_hk AS openaire_researchproduct_hk,
        match.researchproduct_id AS openaire_researchproduct_id,
        publication.publication_date AS openaire_publication_date,
        publication.type AS openaire_type,
        publication.best_access_right AS openaire_access_right,
        publication.best_access_right_uri AS openaire_access_right_uri,
        publication.citation_count AS openaire_citation_count
    FROM unique_originalid_match AS match
    INNER JOIN {{ ref('fct_conicet_openaire_researchproduct_publication') }} AS publication
        USING (researchproduct_hk)
),

final AS (
    SELECT
        base.record_hk,
        base.record_id,
        'oai'::text AS source_system,
        base.title,
        CASE
            WHEN base.valid_date_issued THEN base.date_issued
        END AS publication_date,
        CASE
            WHEN base.valid_date_issued THEN EXTRACT(YEAR FROM base.date_issued)::integer
        END AS publication_year,
        dim_publication_type.publication_type,
        dim_publication_type.publication_type_uri,
        COALESCE(dim_publication_type.publication_type_count, 0) AS publication_type_count,
        COALESCE(dim_publication_type.has_multiple_publication_type, false) AS has_multiple_publication_type,
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
        base.mult_fos_flag AS has_multiple_subject_assignments,
        (openaire_enrichment.openaire_researchproduct_hk IS NOT NULL) AS matched_by_unique_original_id,
        openaire_enrichment.openaire_original_id,
        openaire_enrichment.openaire_researchproduct_hk,
        openaire_enrichment.openaire_researchproduct_id,
        openaire_enrichment.openaire_publication_date,
        openaire_enrichment.openaire_type,
        openaire_enrichment.openaire_access_right,
        openaire_enrichment.openaire_access_right_uri,
        openaire_enrichment.openaire_citation_count
    FROM base
    LEFT JOIN dim_publication_type USING (record_hk)
    LEFT JOIN dim_access_right USING (record_hk)
    LEFT JOIN openaire_enrichment USING (record_hk)
)

SELECT * FROM final
