{{ config(materialized='table') }}

WITH ir_base AS (
    SELECT
        item_hk,
        item_id,
        dc_identifier_uri,
        dc_identifier_uri_raw,
        date_issued,
        date_accessioned,
        title AS ir_title,
        type AS ir_type,
        has_doi AS ir_has_doi,
        has_handle AS ir_has_handle,
        doi_count AS ir_doi_count,
        handle_count AS ir_handle_count,
        doi AS ir_doi,
        handle AS ir_handle,
        in_archive,
        withdrawn,
        discoverable
    FROM {{ ref('fct_unlp_ir_item_publication') }}
    WHERE in_archive = TRUE AND withdrawn = FALSE
),

ir_pid AS (
    SELECT DISTINCT
        item_hk,
        'handle'::text AS scheme,
        pid_value
    FROM ir_base
    CROSS JOIN LATERAL REGEXP_SPLIT_TO_TABLE(COALESCE(ir_handle, ''), '[|]') AS pid_value
    WHERE pid_value <> ''
    UNION
    SELECT DISTINCT
        item_hk,
        'doi'::text AS scheme,
        pid_value
    FROM ir_base
    CROSS JOIN LATERAL REGEXP_SPLIT_TO_TABLE(COALESCE(ir_doi, ''), '[|]') AS pid_value
    WHERE pid_value <> ''
),

ir_publication AS (
    SELECT *
    FROM ir_base
),

openaire_pid_raw AS (
    SELECT DISTINCT
        oa.researchproduct_hk,
        oa.researchproduct_id,
        LOWER(pid.scheme) AS scheme,
        pid.value
    FROM {{ ref('fct_unlp_openaire_researchproduct') }} oa
    JOIN {{ ref('brg_openaire_researchproduct_pid') }} pid USING (researchproduct_hk)
    WHERE LOWER(pid.scheme) IN ('doi', 'handle', 'pmid')
),

openaire_pid AS (
    SELECT DISTINCT
        researchproduct_hk,
        researchproduct_id,
        scheme,
        CASE
            WHEN scheme = 'doi' THEN REGEXP_REPLACE(
                REGEXP_REPLACE(LOWER(value), '^https?://(dx[.])?doi.org/', ''),
                '[\.\),;:]+$',
                ''
            )
            WHEN scheme = 'handle' THEN LOWER(REGEXP_REPLACE(value, '^https?://hdl.handle.net/', ''))
            WHEN scheme = 'pmid' THEN REGEXP_REPLACE(value, '[^0-9]', '', 'g')
        END AS pid_value
    FROM openaire_pid_raw
),

openaire_pid_clean AS (
    SELECT *
    FROM openaire_pid
    WHERE pid_value IS NOT NULL AND pid_value <> ''
),

openaire_pid_agg AS (
    SELECT
        researchproduct_hk,
        COUNT(*) FILTER (WHERE scheme = 'doi') AS openaire_doi_count,
        COUNT(*) FILTER (WHERE scheme = 'handle') AS openaire_handle_count,
        COUNT(*) FILTER (WHERE scheme = 'pmid') AS openaire_pmid_count,
        STRING_AGG(pid_value, '|' ORDER BY pid_value) FILTER (WHERE scheme = 'doi') AS openaire_doi,
        STRING_AGG(pid_value, '|' ORDER BY pid_value) FILTER (WHERE scheme = 'handle') AS openaire_handle,
        STRING_AGG(pid_value, '|' ORDER BY pid_value) FILTER (WHERE scheme = 'pmid') AS openaire_pmid
    FROM openaire_pid_clean
    GROUP BY researchproduct_hk
),

openaire_publication AS (
    SELECT
        oa.*,
        COALESCE(pid_agg.openaire_doi_count, 0) AS openaire_doi_count,
        COALESCE(pid_agg.openaire_handle_count, 0) AS openaire_handle_count,
        COALESCE(pid_agg.openaire_pmid_count, 0) AS openaire_pmid_count,
        COALESCE(pid_agg.openaire_doi_count, 0) > 0 AS openaire_has_doi,
        COALESCE(pid_agg.openaire_handle_count, 0) > 0 AS openaire_has_handle,
        COALESCE(pid_agg.openaire_pmid_count, 0) > 0 AS openaire_has_pmid,
        pid_agg.openaire_doi,
        pid_agg.openaire_handle,
        pid_agg.openaire_pmid
    FROM {{ ref('fct_unlp_openaire_researchproduct') }} oa
    LEFT JOIN openaire_pid_agg pid_agg USING (researchproduct_hk)
),

candidate_match AS (
    SELECT DISTINCT
        ir_pid.item_hk,
        openaire_pid_clean.researchproduct_hk,
        ir_pid.scheme AS match_scheme,
        ir_pid.pid_value AS match_pid,
        CASE
            WHEN ir_pid.scheme = 'doi' THEN 300
            WHEN ir_pid.scheme = 'handle' THEN 200
            WHEN ir_pid.scheme = 'pmid' THEN 100
            ELSE 0
        END AS match_score
    FROM ir_pid
    JOIN openaire_pid_clean
      ON openaire_pid_clean.scheme = ir_pid.scheme
     AND openaire_pid_clean.pid_value = ir_pid.pid_value
),

candidate_match_ranked AS (
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY item_hk) AS item_candidate_match_count,
        COUNT(*) OVER (PARTITION BY researchproduct_hk) AS openaire_candidate_match_count,
        ROW_NUMBER() OVER (
            PARTITION BY item_hk
            ORDER BY match_score DESC, researchproduct_hk, match_pid
        ) AS item_match_rank,
        ROW_NUMBER() OVER (
            PARTITION BY researchproduct_hk
            ORDER BY match_score DESC, item_hk, match_pid
        ) AS openaire_match_rank
    FROM candidate_match
),

best_match AS (
    SELECT
        item_hk,
        researchproduct_hk,
        match_scheme,
        match_pid,
        match_score,
        item_candidate_match_count,
        openaire_candidate_match_count
    FROM candidate_match_ranked
    WHERE item_match_rank = 1
      AND openaire_match_rank = 1
),

matched AS (
    SELECT
        MD5(COALESCE(ir.item_hk, '') || '|' || COALESCE(oa.researchproduct_hk, '')) AS publication_hk,
        'matched'::text AS publication_source,
        TRUE AS has_ir,
        TRUE AS has_openaire,
        match_scheme,
        match_pid,
        match_score,
        item_candidate_match_count,
        openaire_candidate_match_count,

        ir.item_hk,
        ir.item_id,
        ir.dc_identifier_uri,
        ir.date_issued,
        ir.date_accessioned,
        ir.ir_type,
        ir.ir_title,
        ir.discoverable,
        ir.ir_has_doi,
        ir.ir_has_handle,
        ir.ir_doi_count,
        ir.ir_handle_count,
        ir.ir_doi,
        ir.ir_handle,

        oa.researchproduct_hk,
        oa.researchproduct_id,
        oa.organization_ror,
        oa.unlp_first_extract_datetime,
        oa.unlp_last_extract_datetime,
        oa.publication_date,
        oa.type AS openaire_type,
        oa.main_title AS openaire_title,
        oa.publisher,
        oa.best_access_right,
        oa.has_sdg,
        oa.sdg_count,
        oa.sdg_values,
        oa.openaire_has_doi,
        oa.openaire_has_handle,
        oa.openaire_has_pmid,
        oa.openaire_doi_count,
        oa.openaire_handle_count,
        oa.openaire_pmid_count,
        oa.openaire_doi,
        oa.openaire_handle,
        oa.openaire_pmid
    FROM best_match m
    JOIN ir_publication ir USING (item_hk)
    JOIN openaire_publication oa USING (researchproduct_hk)
),

ir_only AS (
    SELECT
        MD5(COALESCE(ir.item_hk, '') || '|') AS publication_hk,
        'ir_only'::text AS publication_source,
        TRUE AS has_ir,
        FALSE AS has_openaire,
        NULL::text AS match_scheme,
        NULL::text AS match_pid,
        NULL::int AS match_score,
        NULL::bigint AS item_candidate_match_count,
        NULL::bigint AS openaire_candidate_match_count,

        ir.item_hk,
        ir.item_id,
        ir.dc_identifier_uri,
        ir.date_issued,
        ir.date_accessioned,
        ir.ir_type,
        ir.ir_title,
        ir.discoverable,
        ir.ir_has_doi,
        ir.ir_has_handle,
        ir.ir_doi_count,
        ir.ir_handle_count,
        ir.ir_doi,
        ir.ir_handle,

        NULL::bytea AS researchproduct_hk,
        NULL::text AS researchproduct_id,
        NULL::text AS organization_ror,
        NULL::timestamp AS unlp_first_extract_datetime,
        NULL::timestamp AS unlp_last_extract_datetime,
        NULL::date AS publication_date,
        NULL::text AS openaire_type,
        NULL::text AS openaire_title,
        NULL::text AS publisher,
        NULL::text AS best_access_right,
        NULL::boolean AS has_sdg,
        NULL::bigint AS sdg_count,
        NULL::text AS sdg_values,
        NULL::boolean AS openaire_has_doi,
        NULL::boolean AS openaire_has_handle,
        NULL::boolean AS openaire_has_pmid,
        NULL::bigint AS openaire_doi_count,
        NULL::bigint AS openaire_handle_count,
        NULL::bigint AS openaire_pmid_count,
        NULL::text AS openaire_doi,
        NULL::text AS openaire_handle,
        NULL::text AS openaire_pmid
    FROM ir_publication ir
    LEFT JOIN best_match bm USING (item_hk)
    WHERE bm.item_hk IS NULL
),

openaire_only AS (
    SELECT
        MD5('|' || COALESCE(oa.researchproduct_hk, '')) AS publication_hk,
        'openaire_only'::text AS publication_source,
        FALSE AS has_ir,
        TRUE AS has_openaire,
        NULL::text AS match_scheme,
        NULL::text AS match_pid,
        NULL::int AS match_score,
        NULL::bigint AS item_candidate_match_count,
        NULL::bigint AS openaire_candidate_match_count,

        NULL::bytea AS item_hk,
        NULL::integer AS item_id,
        NULL::text AS dc_identifier_uri,
        NULL::date AS date_issued,
        NULL::date AS date_accessioned,
        NULL::text AS ir_type,
        NULL::text AS ir_title,
        NULL::boolean AS discoverable,
        NULL::boolean AS ir_has_doi,
        NULL::boolean AS ir_has_handle,
        NULL::bigint AS ir_doi_count,
        NULL::bigint AS ir_handle_count,
        NULL::text AS ir_doi,
        NULL::text AS ir_handle,

        oa.researchproduct_hk,
        oa.researchproduct_id,
        oa.organization_ror,
        oa.unlp_first_extract_datetime,
        oa.unlp_last_extract_datetime,
        oa.publication_date,
        oa.type AS openaire_type,
        oa.main_title AS openaire_title,
        oa.publisher,
        oa.best_access_right,
        oa.has_sdg,
        oa.sdg_count,
        oa.sdg_values,
        oa.openaire_has_doi,
        oa.openaire_has_handle,
        oa.openaire_has_pmid,
        oa.openaire_doi_count,
        oa.openaire_handle_count,
        oa.openaire_pmid_count,
        oa.openaire_doi,
        oa.openaire_handle,
        oa.openaire_pmid
    FROM openaire_publication oa
    LEFT JOIN best_match bm USING (researchproduct_hk)
    WHERE bm.researchproduct_hk IS NULL
),

final AS (
    SELECT
        publication_hk,
        publication_source,
        has_ir,
        has_openaire,
        match_scheme,
        match_pid,
        match_score,
        item_candidate_match_count,
        openaire_candidate_match_count,

        item_hk,
        item_id,
        dc_identifier_uri,
        date_issued,
        date_accessioned,
        ir_type,
        ir_title,
        discoverable,
        ir_has_doi,
        ir_has_handle,
        ir_doi_count,
        ir_handle_count,
        ir_doi,
        ir_handle,

        researchproduct_hk,
        researchproduct_id,
        organization_ror,
        unlp_first_extract_datetime,
        unlp_last_extract_datetime,
        publication_date,
        openaire_type,
        openaire_title,
        publisher,
        best_access_right,
        has_sdg,
        sdg_count,
        sdg_values,
        openaire_has_doi,
        openaire_has_handle,
        openaire_has_pmid,
        openaire_doi_count,
        openaire_handle_count,
        openaire_pmid_count,
        openaire_doi,
        openaire_handle,
        openaire_pmid,

        COALESCE(ir_title, openaire_title) AS title,
        COALESCE(publication_date, date_issued) AS publication_date_best,
        COALESCE(ir_type, openaire_type) AS publication_type_best
    FROM matched

    UNION ALL

    SELECT
        publication_hk,
        publication_source,
        has_ir,
        has_openaire,
        match_scheme,
        match_pid,
        match_score,
        item_candidate_match_count,
        openaire_candidate_match_count,

        item_hk,
        item_id,
        dc_identifier_uri,
        date_issued,
        date_accessioned,
        ir_type,
        ir_title,
        discoverable,
        ir_has_doi,
        ir_has_handle,
        ir_doi_count,
        ir_handle_count,
        ir_doi,
        ir_handle,

        researchproduct_hk,
        researchproduct_id,
        organization_ror,
        unlp_first_extract_datetime,
        unlp_last_extract_datetime,
        publication_date,
        openaire_type,
        openaire_title,
        publisher,
        best_access_right,
        has_sdg,
        sdg_count,
        sdg_values,
        openaire_has_doi,
        openaire_has_handle,
        openaire_has_pmid,
        openaire_doi_count,
        openaire_handle_count,
        openaire_pmid_count,
        openaire_doi,
        openaire_handle,
        openaire_pmid,

        COALESCE(ir_title, openaire_title) AS title,
        COALESCE(publication_date, date_issued) AS publication_date_best,
        COALESCE(ir_type, openaire_type) AS publication_type_best
    FROM ir_only

    UNION ALL

    SELECT
        publication_hk,
        publication_source,
        has_ir,
        has_openaire,
        match_scheme,
        match_pid,
        match_score,
        item_candidate_match_count,
        openaire_candidate_match_count,

        item_hk,
        item_id,
        dc_identifier_uri,
        date_issued,
        date_accessioned,
        ir_type,
        ir_title,
        discoverable,
        ir_has_doi,
        ir_has_handle,
        ir_doi_count,
        ir_handle_count,
        ir_doi,
        ir_handle,

        researchproduct_hk,
        researchproduct_id,
        organization_ror,
        unlp_first_extract_datetime,
        unlp_last_extract_datetime,
        publication_date,
        openaire_type,
        openaire_title,
        publisher,
        best_access_right,
        has_sdg,
        sdg_count,
        sdg_values,
        openaire_has_doi,
        openaire_has_handle,
        openaire_has_pmid,
        openaire_doi_count,
        openaire_handle_count,
        openaire_pmid_count,
        openaire_doi,
        openaire_handle,
        openaire_pmid,

        COALESCE(ir_title, openaire_title) AS title,
        COALESCE(publication_date, date_issued) AS publication_date_best,
        COALESCE(ir_type, openaire_type) AS publication_type_best
    FROM openaire_only
)

SELECT * FROM final
