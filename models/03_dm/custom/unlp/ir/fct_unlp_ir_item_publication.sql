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
        WHERE mv.text_value ~ '^[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$'
    ) AS iso_date,
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
        WHERE mv.text_value ~ '^[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$'
    ) AS iso_date,
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

final as (
    SELECT 
        item.item_hk,
        item.item_id,
        id.dc_identifier_uri,
        id.dc_identifier_uri_raw,

        date_accessioned.raw_values AS date_accessioned_raw,
        CASE
            WHEN date_accessioned.iso_date IS NOT NULL THEN TO_DATE(date_accessioned.iso_date, 'YYYY-MM-DD')
            WHEN date_accessioned.year_only IS NOT NULL THEN TO_DATE(date_accessioned.year_only || '-01-01', 'YYYY-MM-DD')
        END AS date_accessioned,

        date_issued.raw_values AS date_issued_raw,
        CASE
            WHEN date_issued.iso_date IS NOT NULL THEN TO_DATE(date_issued.iso_date, 'YYYY-MM-DD')
            WHEN date_issued.year_only IS NOT NULL THEN TO_DATE(date_issued.year_only || '-01-01', 'YYYY-MM-DD')
        END AS date_issued,

        dc_type.text_value as type,
        in_archive,
        withdrawn,
        discoverable
    FROM {{ref('fct_dspace5_item')}} item
    JOIN id USING (item_hk)
    JOIN dc_type USING (item_hk)
    LEFT JOIN date_accessioned USING (item_hk)
    LEFT JOIN date_issued USING (item_hk)
)

SELECT * FROM final
