{{ config(materialized='view') }}

WITH ranked AS (
    SELECT
        item_id,
        metadata_value_id,
        text_value_raw AS value_raw,
        NULLIF(REPLACE(text_value, '|', ' '), '') AS value,
        ROW_NUMBER() OVER (
            PARTITION BY item_id
            ORDER BY metadata_value_id DESC
        ) AS value_rank
    FROM {{ ref('int_unlp_sedicidb_item_metadatavalue') }}
    WHERE metadatafield_fullname = 'dc.type'
      AND NULLIF(REPLACE(text_value, '|', ' '), '') IS NOT NULL
)

SELECT
    item_id,
    metadata_value_id,
    value_raw,
    value
FROM ranked
WHERE value_rank = 1
