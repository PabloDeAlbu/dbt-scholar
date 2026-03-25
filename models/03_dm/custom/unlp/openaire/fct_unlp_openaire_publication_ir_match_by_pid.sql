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
        openaire_pid_clean.researchproduct_hk,
        ir_pid.item_hk,
        ir_pid.scheme AS match_scheme,
        ir_pid.pid_value AS match_pid,
        CASE
            WHEN ir_pid.scheme = 'doi' THEN 300
            WHEN ir_pid.scheme = 'handle' THEN 200
            WHEN ir_pid.scheme = 'pmid' THEN 100
            ELSE 0
        END AS match_score
    FROM openaire_pid_clean
    JOIN ir_pid
      ON ir_pid.scheme = openaire_pid_clean.scheme
     AND ir_pid.pid_value = openaire_pid_clean.pid_value
),

candidate_match_ranked AS (
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY researchproduct_hk) AS ir_candidate_match_count,
        ROW_NUMBER() OVER (
            PARTITION BY researchproduct_hk
            ORDER BY match_score DESC, item_hk, match_pid
        ) AS researchproduct_match_rank
    FROM candidate_match
),

best_match AS (
    SELECT
        researchproduct_hk,
        item_hk,
        match_scheme,
        match_pid,
        match_score,
        ir_candidate_match_count
    FROM candidate_match_ranked
    WHERE researchproduct_match_rank = 1
),

final AS (
    SELECT
        MD5(COALESCE(oa.researchproduct_hk, '')) AS publication_hk,
        oa.researchproduct_hk,
        oa.researchproduct_id,
        oa.organization_ror,
        oa.unlp_first_extract_datetime,
        oa.unlp_last_extract_datetime,
        oa.publication_date,
        oa.openaire_type,
        oa.openaire_title,
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
        oa.openaire_pmid,

        bm.item_hk,
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

        bm.item_hk IS NOT NULL AS has_ir_pid_match,
        bm.match_scheme,
        bm.match_pid,
        bm.match_score,
        bm.ir_candidate_match_count,

        COALESCE(ir.ir_title, oa.openaire_title) AS title,
        COALESCE(oa.publication_date, ir.date_issued) AS publication_date_best,
        COALESCE(ir.ir_type, oa.openaire_type) AS publication_type_best
    FROM openaire_publication oa
    LEFT JOIN best_match bm USING (researchproduct_hk)
    LEFT JOIN ir_base ir USING (item_hk)
)

SELECT * FROM final
