{{ config(materialized='table') }}

WITH id AS (
    SELECT
        i_mv.item_hk,
        MIN(mv.text_value) AS dc_identifier_uri,
        STRING_AGG(mv.text_value, '|' ORDER BY mv.text_value) AS dc_identifier_uri_raw
    FROM {{ ref('brg_dspace5_item_metadatavalue') }} i_mv
    JOIN {{ ref('dim_dspace5_metadatavalue') }} mv USING (metadatavalue_hk)
    WHERE
        i_mv.metadatafield_fullname = 'dc.identifier.uri' AND
        mv.text_value LIKE 'http://sedici.unlp.edu.ar/handle/10915%'
    GROUP BY i_mv.item_hk
),

date_accessioned AS (
  SELECT 
    i_mv.item_hk,
    MIN(mv.text_value) FILTER (
        WHERE mv.text_value ~ '^[0-9]{4}[-/](0[1-9]|1[0-2])[-/](0[1-9]|[12][0-9]|3[01])'
    ) AS date_ymd_any,
    MIN(mv.text_value) FILTER (
        WHERE mv.text_value ~ '^[0-9]{4}[-/](0[1-9]|1[0-2])$'
    ) AS date_ym_any,
    MIN(mv.text_value) FILTER (
        WHERE mv.text_value ~ '^[0-9]{4}$'
    ) AS year_only,
    STRING_AGG(mv.text_value, '|' ORDER BY mv.text_value) AS raw_values
  FROM {{ ref('brg_dspace5_item_metadatavalue') }} i_mv
  JOIN {{ ref('dim_dspace5_metadatavalue') }} mv USING (metadatavalue_hk)
  WHERE i_mv.metadatafield_fullname = 'dc.date.accessioned'
  GROUP BY i_mv.item_hk
),

date_issued AS (
  SELECT 
    i_mv.item_hk,
    MIN(mv.text_value) FILTER (
        WHERE mv.text_value ~ '^[0-9]{4}[-/](0[1-9]|1[0-2])[-/](0[1-9]|[12][0-9]|3[01])'
    ) AS date_ymd_any,
    MIN(mv.text_value) FILTER (
        WHERE mv.text_value ~ '^[0-9]{4}[-/](0[1-9]|1[0-2])$'
    ) AS date_ym_any,
    MIN(mv.text_value) FILTER (
        WHERE mv.text_value ~ '^[0-9]{4}$'
    ) AS year_only,
    STRING_AGG(mv.text_value, '|' ORDER BY mv.text_value) AS raw_values
  FROM {{ ref('brg_dspace5_item_metadatavalue') }} i_mv
  JOIN {{ ref('dim_dspace5_metadatavalue') }} mv USING (metadatavalue_hk)
  WHERE i_mv.metadatafield_fullname = 'dc.date.issued'
  GROUP BY i_mv.item_hk
),

dc_type AS (
    SELECT
        i_mv.item_hk,
        mv.text_value
    FROM {{ ref('brg_dspace5_item_metadatavalue') }} i_mv
    JOIN {{ ref('dim_dspace5_metadatavalue') }} mv USING (metadatavalue_hk)
    WHERE i_mv.metadatafield_fullname = 'dc.type'
),

ir_title AS (
    SELECT
        i_mv.item_hk,
        MIN(mv.text_value)::text AS title
    FROM {{ ref('brg_dspace5_item_metadatavalue') }} i_mv
    JOIN {{ ref('dim_dspace5_metadatavalue') }} mv USING (metadatavalue_hk)
    WHERE i_mv.metadatafield_fullname = 'dc.title'
    GROUP BY i_mv.item_hk
),

ir_doi_raw AS (
    SELECT
        i_mv.item_hk,
        LOWER(mv.text_value)::text AS raw_value
    FROM {{ ref('brg_dspace5_item_metadatavalue') }} i_mv
    JOIN {{ ref('dim_dspace5_metadatavalue') }} mv USING (metadatavalue_hk)
    WHERE i_mv.metadatafield_fullname IN ('dc.identifier.uri', 'sedici.identifier.other')
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
    ) pid
    GROUP BY item_hk
),

final as (
    SELECT 
        item.item_hk,
        item.item_id,
        id.dc_identifier_uri,
        id.dc_identifier_uri_raw,

        date_accessioned.raw_values AS date_accessioned_raw,
        CASE
            WHEN date_accessioned.date_ymd_any IS NOT NULL THEN TO_DATE(REPLACE(LEFT(date_accessioned.date_ymd_any, 10), '/', '-'), 'YYYY-MM-DD')
            WHEN date_accessioned.date_ym_any IS NOT NULL THEN TO_DATE(REPLACE(date_accessioned.date_ym_any, '/', '-') || '-01', 'YYYY-MM-DD')
            WHEN date_accessioned.year_only IS NOT NULL THEN TO_DATE(date_accessioned.year_only || '-01-01', 'YYYY-MM-DD')
        END AS date_accessioned,

        date_issued.raw_values AS date_issued_raw,
        CASE
            WHEN date_issued.date_ymd_any IS NOT NULL THEN TO_DATE(REPLACE(LEFT(date_issued.date_ymd_any, 10), '/', '-'), 'YYYY-MM-DD')
            WHEN date_issued.date_ym_any IS NOT NULL THEN TO_DATE(REPLACE(date_issued.date_ym_any, '/', '-') || '-01', 'YYYY-MM-DD')
            WHEN date_issued.year_only IS NOT NULL THEN TO_DATE(date_issued.year_only || '-01-01', 'YYYY-MM-DD')
        END AS date_issued,

        dc_type.text_value as type,
        ir_title.title,
        COALESCE(ir_pid_agg.doi_count, 0) AS doi_count,
        COALESCE(ir_pid_agg.handle_count, 0) AS handle_count,
        COALESCE(ir_pid_agg.doi_count, 0) > 0 AS has_doi,
        COALESCE(ir_pid_agg.handle_count, 0) > 0 AS has_handle,
        ir_pid_agg.doi,
        ir_pid_agg.handle,
        in_archive,
        withdrawn,
        discoverable
    FROM {{ref('fct_dspace5_item')}} item
    JOIN id USING (item_hk)
    JOIN dc_type USING (item_hk)
    LEFT JOIN ir_title USING (item_hk)
    LEFT JOIN ir_pid_agg USING (item_hk)
    LEFT JOIN date_accessioned USING (item_hk)
    LEFT JOIN date_issued USING (item_hk)
)

SELECT * FROM final
