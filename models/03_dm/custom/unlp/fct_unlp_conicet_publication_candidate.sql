{{ config(materialized='table') }}

WITH center_map AS (
    SELECT
        set_spec,
        set_name,
        sedici_collection_uri,
        faculty,
        pdf_share_pct,
        sedici_article_count,
        manual_status,
        ingest_note
    FROM {{ ref('ldg_unlp_centros_conicet') }}
),

conicet_base AS (
    SELECT
        record_hk,
        record_id,
        title,
        publication_date,
        publication_year,
        publication_type,
        publication_type_uri,
        source_system,
        access_right,
        access_right_uri,
        institutional_uri,
        doi,
        has_doi,
        doi_count,
        set_spec,
        matched_by_unique_original_id,
        openaire_original_id,
        openaire_researchproduct_hk,
        openaire_researchproduct_id,
        openaire_access_right,
        openaire_access_right_uri,
        openaire_is_green,
        openaire_has_doi,
        openaire_doi_count
    FROM {{ ref('fct_conicet_publication') }}
    WHERE set_spec IS NOT NULL
),

conicet_set AS (
    SELECT DISTINCT
        base.record_hk,
        base.record_id,
        base.title,
        base.publication_date,
        base.publication_year,
        base.publication_type,
        base.publication_type_uri,
        base.source_system,
        base.access_right,
        base.access_right_uri,
        base.institutional_uri,
        base.doi,
        base.has_doi,
        base.doi_count,
        NULLIF(BTRIM(set_value), '') AS set_spec,
        base.matched_by_unique_original_id,
        base.openaire_original_id,
        base.openaire_researchproduct_hk,
        base.openaire_researchproduct_id,
        base.openaire_access_right,
        base.openaire_access_right_uri,
        base.openaire_is_green,
        base.openaire_has_doi,
        base.openaire_doi_count
    FROM conicet_base AS base
    CROSS JOIN LATERAL REGEXP_SPLIT_TO_TABLE(COALESCE(base.set_spec, ''), '[|]') AS set_value
    WHERE NULLIF(BTRIM(set_value), '') IS NOT NULL
),

final AS (
    SELECT
        conicet_set.record_hk,
        conicet_set.record_id,
        conicet_set.set_spec,
        center_map.set_name,
        center_map.sedici_collection_uri,
        center_map.faculty,
        conicet_set.title,
        conicet_set.publication_date,
        conicet_set.publication_year,
        conicet_set.publication_type,
        conicet_set.publication_type_uri,
        conicet_set.source_system,
        conicet_set.access_right,
        conicet_set.access_right_uri,
        conicet_set.openaire_access_right,
        conicet_set.openaire_access_right_uri,
        conicet_set.openaire_is_green,
        conicet_set.institutional_uri,
        conicet_set.doi,
        conicet_set.has_doi,
        conicet_set.doi_count,
        conicet_set.matched_by_unique_original_id,
        conicet_set.openaire_original_id,
        conicet_set.openaire_researchproduct_hk,
        conicet_set.openaire_researchproduct_id,
        center_map.pdf_share_pct,
        center_map.sedici_article_count,
        center_map.manual_status,
        center_map.ingest_note,
        TRUE AS is_in_unlp_conicet_center,
        COALESCE((
            conicet_set.access_right_uri = 'http://purl.org/coar/access_right/c_abf2'
            OR conicet_set.openaire_access_right_uri = 'http://purl.org/coar/access_right/c_abf2'
            OR conicet_set.access_right = 'OPEN ACCESS'
            OR conicet_set.openaire_access_right = 'OPEN ACCESS'
        ), FALSE) AS is_open_access_candidate
    FROM conicet_set
    INNER JOIN center_map USING (set_spec)
)

SELECT * FROM final
