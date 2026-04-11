{{ config(materialized='table') }}

WITH base AS (
    SELECT *
    FROM {{ ref('fct_dspacedb5_item_publication') }}
    WHERE institution_ror = 'https://ror.org/01tjs6929'
),

owning_community AS (
    SELECT
        item_hk,
        collection_hk AS owningcollection_hk,
        collection_title AS owning_collection_title,
        community_hk AS owning_community_hk,
        community_id AS owning_community_id,
        community_title AS owning_community_title,
        root_community_hk AS owning_root_community_hk,
        root_community_id AS owning_root_community_id,
        root_community_title AS owning_root_community_title,
        community_path_ids AS owning_community_path_ids,
        community_path_titles AS owning_community_path_titles,
        ROW_NUMBER() OVER (
            PARTITION BY item_hk
            ORDER BY community_path_titles, community_id, collection_id
        ) AS rn
    FROM {{ ref('fct_unlp_ir_item_community') }}
    WHERE is_owning_collection
),

id AS (
    SELECT
        b.item_hk,
        MIN(mv.text_value) AS dc_identifier_uri,
        STRING_AGG(DISTINCT mv.text_value, '|' ORDER BY mv.text_value) AS dc_identifier_uri_raw
    FROM base AS b
    JOIN {{ ref('fct_dspacedb5_item_metadata') }} AS mv
      ON mv.item_id = b.item_id
     AND mv.institution_ror = b.institution_ror
    WHERE mv.metadatafield_fullname = 'dc.identifier.uri'
      AND mv.text_value ~ '^https?://sedici[.]unlp[.]edu[.]ar/handle/10915'
    GROUP BY b.item_hk
),

date_accessioned AS (
    SELECT
        b.item_hk,
        MIN(mv.text_value) FILTER (
            WHERE mv.text_value ~ '^[0-9]{4}[-/](0[1-9]|1[0-2])[-/](0[1-9]|[12][0-9]|3[01])'
        ) AS date_ymd_any,
        MIN(mv.text_value) FILTER (
            WHERE mv.text_value ~ '^[0-9]{4}[-/](0[1-9]|1[0-2])$'
        ) AS date_ym_any,
        MIN(mv.text_value) FILTER (
            WHERE mv.text_value ~ '^[0-9]{4}$'
        ) AS year_only,
        STRING_AGG(DISTINCT mv.text_value, '|' ORDER BY mv.text_value) AS raw_values
    FROM base AS b
    JOIN {{ ref('fct_dspacedb5_item_metadata') }} AS mv
      ON mv.item_id = b.item_id
     AND mv.institution_ror = b.institution_ror
    WHERE mv.metadatafield_fullname = 'dc.date.accessioned'
    GROUP BY b.item_hk
),

date_available AS (
    SELECT
        b.item_hk,
        MIN(mv.text_value) FILTER (
            WHERE mv.text_value ~ '^[0-9]{4}[-/](0[1-9]|1[0-2])[-/](0[1-9]|[12][0-9]|3[01])'
        ) AS date_ymd_any,
        MIN(mv.text_value) FILTER (
            WHERE mv.text_value ~ '^[0-9]{4}[-/](0[1-9]|1[0-2])$'
        ) AS date_ym_any,
        MIN(mv.text_value) FILTER (
            WHERE mv.text_value ~ '^[0-9]{4}$'
        ) AS year_only,
        STRING_AGG(DISTINCT mv.text_value, '|' ORDER BY mv.text_value) AS raw_values
    FROM base AS b
    JOIN {{ ref('fct_dspacedb5_item_metadata') }} AS mv
      ON mv.item_id = b.item_id
     AND mv.institution_ror = b.institution_ror
    WHERE mv.metadatafield_fullname = 'dc.date.available'
    GROUP BY b.item_hk
),

date_issued AS (
    SELECT
        b.item_hk,
        MIN(mv.text_value) FILTER (
            WHERE mv.text_value ~ '^[0-9]{4}[-/](0[1-9]|1[0-2])[-/](0[1-9]|[12][0-9]|3[01])'
        ) AS date_ymd_any,
        MIN(mv.text_value) FILTER (
            WHERE mv.text_value ~ '^[0-9]{4}[-/](0[1-9]|1[0-2])$'
        ) AS date_ym_any,
        MIN(mv.text_value) FILTER (
            WHERE mv.text_value ~ '^[0-9]{4}$'
        ) AS year_only,
        STRING_AGG(DISTINCT mv.text_value, '|' ORDER BY mv.text_value) AS raw_values
    FROM base AS b
    JOIN {{ ref('fct_dspacedb5_item_metadata') }} AS mv
      ON mv.item_id = b.item_id
     AND mv.institution_ror = b.institution_ror
    WHERE mv.metadatafield_fullname = 'dc.date.issued'
    GROUP BY b.item_hk
),

dc_type AS (
    SELECT
        b.item_hk,
        MIN(mv.text_value)::text AS type
    FROM base AS b
    JOIN {{ ref('fct_dspacedb5_item_metadata') }} AS mv
      ON mv.item_id = b.item_id
     AND mv.institution_ror = b.institution_ror
    WHERE mv.metadatafield_fullname = 'dc.type'
    GROUP BY b.item_hk
),

title AS (
    SELECT
        b.item_hk,
        MIN(mv.text_value)::text AS title
    FROM base AS b
    JOIN {{ ref('fct_dspacedb5_item_metadata') }} AS mv
      ON mv.item_id = b.item_id
     AND mv.institution_ror = b.institution_ror
    WHERE mv.metadatafield_fullname = 'dc.title'
    GROUP BY b.item_hk
),

subtitle AS (
    SELECT
        b.item_hk,
        STRING_AGG(DISTINCT mv.text_value::text, '|' ORDER BY mv.text_value::text) AS subtitle
    FROM base AS b
    JOIN {{ ref('fct_dspacedb5_item_metadata') }} AS mv
      ON mv.item_id = b.item_id
     AND mv.institution_ror = b.institution_ror
    WHERE mv.metadatafield_fullname = 'dc.title.alternative'
    GROUP BY b.item_hk
),

description AS (
    SELECT
        b.item_hk,
        STRING_AGG(DISTINCT mv.text_value::text, '|' ORDER BY mv.text_value::text) AS description
    FROM base AS b
    JOIN {{ ref('fct_dspacedb5_item_metadata') }} AS mv
      ON mv.item_id = b.item_id
     AND mv.institution_ror = b.institution_ror
    WHERE mv.metadatafield_fullname IN ('dc.description.abstract', 'dc.description')
    GROUP BY b.item_hk
),

subject AS (
    SELECT
        b.item_hk,
        STRING_AGG(DISTINCT mv.text_value::text, '|' ORDER BY mv.text_value::text) AS subject
    FROM base AS b
    JOIN {{ ref('fct_dspacedb5_item_metadata') }} AS mv
      ON mv.item_id = b.item_id
     AND mv.institution_ror = b.institution_ror
    WHERE mv.metadatafield_fullname = 'dc.subject'
    GROUP BY b.item_hk
),

subtype AS (
    SELECT
        b.item_hk,
        STRING_AGG(DISTINCT mv.text_value::text, '|' ORDER BY mv.text_value::text) AS subtype
    FROM base AS b
    JOIN {{ ref('fct_dspacedb5_item_metadata') }} AS mv
      ON mv.item_id = b.item_id
     AND mv.institution_ror = b.institution_ror
    WHERE mv.metadatafield_fullname = 'sedici.subtype'
    GROUP BY b.item_hk
),

author AS (
    SELECT
        b.item_hk,
        STRING_AGG(DISTINCT mv.text_value::text, '|' ORDER BY mv.text_value::text) AS author,
        COUNT(DISTINCT NULLIF(TRIM(mv.text_value::text), ''))::int AS author_count
    FROM base AS b
    JOIN {{ ref('fct_dspacedb5_item_metadata') }} AS mv
      ON mv.item_id = b.item_id
     AND mv.institution_ror = b.institution_ror
    WHERE mv.metadatafield_fullname = 'sedici.creator.person'
    GROUP BY b.item_hk
),

issn AS (
    SELECT
        b.item_hk,
        STRING_AGG(DISTINCT mv.text_value::text, '|' ORDER BY mv.text_value::text) AS issn
    FROM base AS b
    JOIN {{ ref('fct_dspacedb5_item_metadata') }} AS mv
      ON mv.item_id = b.item_id
     AND mv.institution_ror = b.institution_ror
    WHERE mv.metadatafield_fullname = 'sedici.identifier.issn'
    GROUP BY b.item_hk
),

isbn AS (
    SELECT
        b.item_hk,
        STRING_AGG(DISTINCT mv.text_value::text, '|' ORDER BY mv.text_value::text) AS isbn
    FROM base AS b
    JOIN {{ ref('fct_dspacedb5_item_metadata') }} AS mv
      ON mv.item_id = b.item_id
     AND mv.institution_ror = b.institution_ror
    WHERE mv.metadatafield_fullname = 'sedici.identifier.isbn'
    GROUP BY b.item_hk
),

ir_doi_raw AS (
    SELECT
        b.item_hk,
        LOWER(mv.text_value)::text AS raw_value
    FROM base AS b
    JOIN {{ ref('fct_dspacedb5_item_metadata') }} AS mv
      ON mv.item_id = b.item_id
     AND mv.institution_ror = b.institution_ror
    WHERE mv.metadatafield_fullname IN ('dc.identifier.uri', 'sedici.identifier.other')
),

ir_doi_pid AS (
    SELECT DISTINCT
        item_hk,
        REGEXP_REPLACE(
            SUBSTRING(raw_value FROM '(10\.[0-9]{4,9}/[^[:space:]<>|";]+)'),
            '[\.\),;:]+$',
            ''
        ) AS pid_value
    FROM ir_doi_raw
    WHERE SUBSTRING(raw_value FROM '(10\.[0-9]{4,9}/[^[:space:]<>|";]+)') IS NOT NULL
),

ir_handle_pid AS (
    SELECT DISTINCT
        item_hk,
        LOWER(REGEXP_REPLACE(dc_identifier_uri, '^https?://[^/]+/handle/', '')) AS pid_value
    FROM id
    WHERE dc_identifier_uri ~* '^https?://[^/]+/handle/'
),

ir_pid_agg AS (
    SELECT
        item_hk,
        COUNT(*) FILTER (WHERE scheme = 'doi') AS doi_count,
        COUNT(*) FILTER (WHERE scheme = 'handle') AS handle_count,
        STRING_AGG(pid_value, '|' ORDER BY pid_value) FILTER (WHERE scheme = 'doi') AS doi,
        STRING_AGG(pid_value, '|' ORDER BY pid_value) FILTER (WHERE scheme = 'handle') AS handle
    FROM (
        SELECT item_hk, 'doi'::text AS scheme, pid_value FROM ir_doi_pid
        UNION
        SELECT item_hk, 'handle'::text AS scheme, pid_value FROM ir_handle_pid
    ) AS pid
    GROUP BY item_hk
),

final AS (
    SELECT
        b.item_hk,
        b.item_id,
        b.source_label,
        b.institution_ror,
        b.submitter_id,
        b.owning_collection,
        b.collections_count,
        b.last_modified,
        b.first_extract_datetime,
        b.last_extract_datetime,
        b.first_load_datetime,
        b.last_load_datetime,

        id.dc_identifier_uri,
        id.dc_identifier_uri_raw,

        date_accessioned.raw_values AS date_accessioned_raw,
        CASE
            WHEN date_accessioned.date_ymd_any IS NOT NULL THEN TO_DATE(REPLACE(LEFT(date_accessioned.date_ymd_any, 10), '/', '-'), 'YYYY-MM-DD')
            WHEN date_accessioned.date_ym_any IS NOT NULL THEN TO_DATE(REPLACE(date_accessioned.date_ym_any, '/', '-') || '-01', 'YYYY-MM-DD')
            WHEN date_accessioned.year_only IS NOT NULL THEN TO_DATE(date_accessioned.year_only || '-01-01', 'YYYY-MM-DD')
        END AS date_accessioned,

        date_available.raw_values AS dc_date_available_raw,
        CASE
            WHEN date_available.date_ymd_any IS NOT NULL THEN TO_DATE(REPLACE(LEFT(date_available.date_ymd_any, 10), '/', '-'), 'YYYY-MM-DD')
            WHEN date_available.date_ym_any IS NOT NULL THEN TO_DATE(REPLACE(date_available.date_ym_any, '/', '-') || '-01', 'YYYY-MM-DD')
            WHEN date_available.year_only IS NOT NULL THEN TO_DATE(date_available.year_only || '-01-01', 'YYYY-MM-DD')
        END AS dc_date_available,

        date_issued.raw_values AS date_issued_raw,
        CASE
            WHEN date_issued.date_ymd_any IS NOT NULL THEN TO_DATE(REPLACE(LEFT(date_issued.date_ymd_any, 10), '/', '-'), 'YYYY-MM-DD')
            WHEN date_issued.date_ym_any IS NOT NULL THEN TO_DATE(REPLACE(date_issued.date_ym_any, '/', '-') || '-01', 'YYYY-MM-DD')
            WHEN date_issued.year_only IS NOT NULL THEN TO_DATE(date_issued.year_only || '-01-01', 'YYYY-MM-DD')
        END AS date_issued,
        CASE
            WHEN date_issued.date_ymd_any IS NOT NULL THEN TO_DATE(REPLACE(LEFT(date_issued.date_ymd_any, 10), '/', '-'), 'YYYY-MM-DD')
            WHEN date_issued.date_ym_any IS NOT NULL THEN TO_DATE(REPLACE(date_issued.date_ym_any, '/', '-') || '-01', 'YYYY-MM-DD')
            WHEN date_issued.year_only IS NOT NULL THEN TO_DATE(date_issued.year_only || '-01-01', 'YYYY-MM-DD')
        END AS dc_date_issued,
        date_issued.raw_values AS dc_date_issued_raw,

        title.title,
        subtitle.subtitle,
        dc_type.type,
        description.description,
        subject.subject,
        subtype.subtype,
        author.author,
        COALESCE(author.author_count, 0) AS author_count,
        issn.issn,
        isbn.isbn,

        COALESCE(ir_pid_agg.doi_count, 0) AS doi_count,
        COALESCE(ir_pid_agg.handle_count, 0) AS handle_count,
        COALESCE(ir_pid_agg.doi_count, 0) > 0 AS has_doi,
        COALESCE(ir_pid_agg.handle_count, 0) > 0 AS has_handle,
        ir_pid_agg.doi,
        ir_pid_agg.handle,
        oc.owningcollection_hk,
        oc.owning_collection_title,
        oc.owning_community_hk,
        oc.owning_community_id,
        oc.owning_community_title,
        oc.owning_root_community_hk,
        oc.owning_root_community_id,
        oc.owning_root_community_title,
        oc.owning_community_path_ids,
        oc.owning_community_path_titles,
        b.in_archive,
        b.withdrawn,
        b.discoverable
    FROM base AS b
    JOIN id USING (item_hk)
    JOIN dc_type USING (item_hk)
    LEFT JOIN title USING (item_hk)
    LEFT JOIN subtitle USING (item_hk)
    LEFT JOIN description USING (item_hk)
    LEFT JOIN subject USING (item_hk)
    LEFT JOIN subtype USING (item_hk)
    LEFT JOIN author USING (item_hk)
    LEFT JOIN issn USING (item_hk)
    LEFT JOIN isbn USING (item_hk)
    LEFT JOIN ir_pid_agg USING (item_hk)
    LEFT JOIN date_accessioned USING (item_hk)
    LEFT JOIN date_available USING (item_hk)
    LEFT JOIN date_issued USING (item_hk)
    LEFT JOIN owning_community AS oc
        ON b.item_hk = oc.item_hk
       AND oc.rn = 1
    WHERE b.discoverable = TRUE
      AND b.in_archive = TRUE
      AND b.withdrawn = FALSE

)

SELECT * FROM final
