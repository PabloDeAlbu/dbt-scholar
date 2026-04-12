{{ config(materialized='table') }}

WITH ir_base AS (
    SELECT
        item_hk,
        item_id,
        source_label,
        institution_ror,
        dc_identifier_uri,
        dc_identifier_uri_raw,
        date_issued,
        date_accessioned,
        title,
        subtitle,
        type,
        description,
        subject,
        subtype,
        author,
        author_count,
        issn,
        isbn,
        has_doi,
        has_handle,
        doi_count,
        handle_count,
        doi,
        handle,
        owningcollection_hk,
        owning_collection_title,
        owning_community_hk,
        owning_community_id,
        owning_community_title,
        owning_root_community_hk,
        owning_root_community_id,
        owning_root_community_title,
        owning_community_path_ids,
        owning_community_path_titles,
        in_archive,
        withdrawn,
        discoverable
    FROM {{ ref('fct_unlp_ir_item_publication') }}
    WHERE discoverable = TRUE
      AND in_archive = TRUE
      AND withdrawn = FALSE
),

ir_doi AS (
    SELECT DISTINCT
        item_hk,
        NULLIF(BTRIM(pid_value), '') AS doi_normalized
    FROM ir_base
    CROSS JOIN LATERAL REGEXP_SPLIT_TO_TABLE(COALESCE(doi, ''), '[|]') AS pid_value
    WHERE NULLIF(BTRIM(pid_value), '') IS NOT NULL
),

ir_doi_stats AS (
    SELECT
        doi_normalized,
        COUNT(DISTINCT item_hk) AS ir_item_count
    FROM ir_doi
    GROUP BY doi_normalized
),

openalex_doi AS (
    SELECT
        work_hk,
        work_id,
        doi AS doi_normalized
    FROM {{ ref('fct_unlp_openalex_work_publication') }}
    WHERE doi IS NOT NULL
      AND TRIM(doi) <> ''
),

openalex_doi_stats AS (
    SELECT
        doi_normalized,
        COUNT(DISTINCT work_hk) AS openalex_work_count
    FROM openalex_doi
    GROUP BY doi_normalized
),

unique_doi_match AS (
    SELECT
        ir_doi.item_hk,
        openalex_doi.work_hk,
        openalex_doi.doi_normalized
    FROM ir_doi
    INNER JOIN openalex_doi USING (doi_normalized)
    INNER JOIN ir_doi_stats USING (doi_normalized)
    INNER JOIN openalex_doi_stats USING (doi_normalized)
    WHERE ir_doi_stats.ir_item_count = 1
      AND openalex_doi_stats.openalex_work_count = 1
),

openalex_enrichment AS (
    SELECT
        match.item_hk,
        match.doi_normalized AS match_doi,
        oa.work_hk AS openalex_work_hk,
        oa.work_id AS openalex_work_id,
        oa.publication_date AS openalex_publication_date,
        oa.publication_year AS openalex_publication_year,
        oa.type AS openalex_type,
        oa.title AS openalex_title,
        oa.institution_ror AS openalex_institution_ror,
        oa.unlp_first_extract_datetime AS openalex_first_extract_datetime,
        oa.unlp_last_extract_datetime AS openalex_last_extract_datetime,
        oa.is_oa AS openalex_is_oa,
        oa.oa_status AS openalex_oa_status,
        oa.oa_url AS openalex_oa_url,
        oa.has_fulltext AS openalex_has_fulltext,
        oa.fulltext_origin AS openalex_fulltext_origin,
        oa.cited_by_count AS openalex_cited_by_count,
        oa.fwci AS openalex_fwci
    FROM unique_doi_match AS match
    INNER JOIN {{ ref('fct_unlp_openalex_work_publication') }} AS oa
        ON oa.work_hk = match.work_hk
),

final AS (
    SELECT
        ir.item_hk AS publication_hk,
        ir.item_hk,
        ir.item_id,
        'ir'::text AS source_system,
        ir.source_label,
        ir.institution_ror,
        ir.dc_identifier_uri AS institutional_uri,
        ir.dc_identifier_uri_raw,
        ir.date_issued AS publication_date,
        EXTRACT(YEAR FROM ir.date_issued)::int AS publication_year,
        ir.date_accessioned,
        ir.title,
        ir.subtitle,
        ir.type AS publication_type,
        ir.description,
        ir.subject,
        ir.subtype,
        ir.author,
        ir.author_count,
        ir.issn,
        ir.isbn,
        ir.has_doi,
        ir.doi_count,
        ir.doi,
        ir.has_handle,
        ir.handle_count,
        ir.handle,
        ir.owningcollection_hk,
        ir.owning_collection_title,
        ir.owning_community_hk,
        ir.owning_community_id,
        ir.owning_community_title,
        ir.owning_root_community_hk,
        ir.owning_root_community_id,
        ir.owning_root_community_title,
        ir.owning_community_path_ids,
        ir.owning_community_path_titles,
        ir.discoverable,
        ir.in_archive,
        ir.withdrawn,

        (oa.openalex_work_hk IS NOT NULL) AS matched_by_unique_doi,
        oa.match_doi,
        oa.openalex_work_hk,
        oa.openalex_work_id,
        oa.openalex_publication_date,
        oa.openalex_publication_year,
        oa.openalex_type,
        oa.openalex_title,
        oa.openalex_institution_ror,
        oa.openalex_first_extract_datetime,
        oa.openalex_last_extract_datetime,
        oa.openalex_is_oa,
        oa.openalex_oa_status,
        oa.openalex_oa_url,
        oa.openalex_has_fulltext,
        oa.openalex_fulltext_origin,
        oa.openalex_cited_by_count,
        oa.openalex_fwci
    FROM ir_base AS ir
    LEFT JOIN openalex_enrichment AS oa USING (item_hk)
)

SELECT * FROM final
