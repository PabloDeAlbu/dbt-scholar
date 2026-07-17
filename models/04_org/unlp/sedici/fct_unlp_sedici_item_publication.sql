{{ config(materialized='table') }}

WITH base AS (
    SELECT *
    FROM {{ ref('fct_dspacedb5_item_publication') }}
    WHERE institution_ror = 'https://ror.org/01tjs6929'
),
metadata_usage AS (
    SELECT
        b.item_hk,
        mu.item_id,
        mu.metadatafield_hk,
        mu.metadatafield_fullname,
        mu.preferred_text_value,
        mu.ordered_text_values,
        mu.distinct_nonempty_text_value_count,
        mu.min_ymd_text_value,
        mu.min_ym_text_value,
        mu.min_year_text_value
    FROM base AS b
    JOIN {{ ref('fct_unlp_sedici_item_metadatafield_usage') }} AS mu
        USING (item_hk)
    WHERE mu.metadatafield_fullname IN (
        'dc.identifier.uri',
        'dc.date.accessioned',
        'dc.date.available',
        'dc.date.issued',
        'dc.type',
        'dc.title',
        'dc.title.alternative',
        'dc.description.abstract',
        'dc.description',
        'dc.subject',
        'sedici.subtype',
        'sedici.creator.person',
        'sedici.creator.corporate',
        'sedici.identifier.issn',
        'sedici.identifier.isbn',
        'sedici.identifier.other',
        'sedici.identifier.doi'
    )
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
    FROM {{ ref('brg_unlp_sedici_item_community') }}
    WHERE is_owning_collection
),

id AS (
    SELECT
        item_hk,
        MIN(value) AS dc_identifier_uri,
        STRING_AGG(DISTINCT value, '|' ORDER BY value) AS dc_identifier_uri_raw
    FROM (
        SELECT
            mu.item_hk,
            BTRIM(value) AS value
        FROM metadata_usage AS mu
        CROSS JOIN LATERAL UNNEST(STRING_TO_ARRAY(COALESCE(mu.ordered_text_values, ''), '|')) AS value
        WHERE mu.metadatafield_fullname = 'dc.identifier.uri'
    ) AS raw
    WHERE value ~ '^https?://sedici[.]unlp[.]edu[.]ar/handle/10915'
    GROUP BY item_hk
),

date_accessioned AS (
    SELECT
        item_hk,
        min_ymd_text_value AS date_ymd_any,
        min_ym_text_value AS date_ym_any,
        min_year_text_value AS year_only,
        ordered_text_values AS raw_values
    FROM metadata_usage
    WHERE metadatafield_fullname = 'dc.date.accessioned'
),

date_available AS (
    SELECT
        item_hk,
        min_ymd_text_value AS date_ymd_any,
        min_ym_text_value AS date_ym_any,
        min_year_text_value AS year_only,
        ordered_text_values AS raw_values
    FROM metadata_usage
    WHERE metadatafield_fullname = 'dc.date.available'
),

date_issued AS (
    SELECT
        item_hk,
        min_ymd_text_value AS date_ymd_any,
        min_ym_text_value AS date_ym_any,
        min_year_text_value AS year_only,
        ordered_text_values AS raw_values
    FROM metadata_usage
    WHERE metadatafield_fullname = 'dc.date.issued'
),

dc_type AS (
    SELECT
        item_hk,
        preferred_text_value::text AS type
    FROM metadata_usage
    WHERE metadatafield_fullname = 'dc.type'
),

title AS (
    SELECT
        item_hk,
        preferred_text_value::text AS title
    FROM metadata_usage
    WHERE metadatafield_fullname = 'dc.title'
),

subtitle AS (
    SELECT
        item_hk,
        ordered_text_values::text AS subtitle
    FROM metadata_usage
    WHERE metadatafield_fullname = 'dc.title.alternative'
),

description AS (
    SELECT
        item_hk,
        STRING_AGG(ordered_text_values::text, '|' ORDER BY metadatafield_fullname) AS description
    FROM metadata_usage
    WHERE metadatafield_fullname IN ('dc.description.abstract', 'dc.description')
    GROUP BY item_hk
),

subject AS (
    SELECT
        item_hk,
        ordered_text_values::text AS subject
    FROM metadata_usage
    WHERE metadatafield_fullname = 'dc.subject'
),

subtype AS (
    SELECT
        item_hk,
        ordered_text_values::text AS subtype
    FROM metadata_usage
    WHERE metadatafield_fullname = 'sedici.subtype'
),

author AS (
    SELECT
        item_hk,
        STRING_AGG(ordered_text_values::text, '|' ORDER BY metadatafield_fullname) AS author,
        SUM(distinct_nonempty_text_value_count)::int AS author_count
    FROM metadata_usage
    WHERE metadatafield_fullname IN ('sedici.creator.person', 'sedici.creator.corporate')
    GROUP BY item_hk
),

issn AS (
    SELECT
        item_hk,
        ordered_text_values::text AS issn
    FROM metadata_usage
    WHERE metadatafield_fullname = 'sedici.identifier.issn'
),

isbn AS (
    SELECT
        item_hk,
        ordered_text_values::text AS isbn
    FROM metadata_usage
    WHERE metadatafield_fullname = 'sedici.identifier.isbn'
),

ir_doi_raw AS (
    SELECT
        mu.item_hk,
        LOWER(BTRIM(value))::text AS raw_value
    FROM metadata_usage AS mu
    CROSS JOIN LATERAL UNNEST(STRING_TO_ARRAY(COALESCE(mu.ordered_text_values, ''), '|')) AS value
    WHERE mu.metadatafield_fullname IN ('dc.identifier.uri', 'sedici.identifier.other', 'sedici.identifier.doi')
      AND BTRIM(value) <> ''
),

ir_doi_pid AS (
    SELECT DISTINCT
        item_hk,
        REGEXP_REPLACE(
            SUBSTRING(raw_value FROM '(10\\.[0-9]{4,9}/[^[:space:]<>|\";]+)'),
            '[\\.\\),;:]+$',
            ''
        ) AS pid_value
    FROM ir_doi_raw
    WHERE SUBSTRING(raw_value FROM '(10\\.[0-9]{4,9}/[^[:space:]<>|\";]+)') IS NOT NULL
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

final_raw AS (
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
),

final_ranked AS (
    SELECT
        final_raw.*,
        ROW_NUMBER() OVER (
            PARTITION BY LOWER(REGEXP_REPLACE(final_raw.dc_identifier_uri, '^https?://[^/]+/handle/', ''))
            ORDER BY
                final_raw.last_modified DESC NULLS LAST,
                final_raw.last_load_datetime DESC NULLS LAST,
                final_raw.item_id DESC
        ) AS handle_rn
    FROM final_raw
)

SELECT
    item_hk,
    item_id,
    source_label,
    institution_ror,
    submitter_id,
    owning_collection,
    collections_count,
    last_modified,
    first_extract_datetime,
    last_extract_datetime,
    first_load_datetime,
    last_load_datetime,
    dc_identifier_uri,
    dc_identifier_uri_raw,
    date_accessioned_raw,
    date_accessioned,
    dc_date_available_raw,
    dc_date_available,
    date_issued_raw,
    date_issued,
    dc_date_issued,
    dc_date_issued_raw,
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
    doi_count,
    handle_count,
    has_doi,
    has_handle,
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
FROM final_ranked
WHERE handle_rn = 1
