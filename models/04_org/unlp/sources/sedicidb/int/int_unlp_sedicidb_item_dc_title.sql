{{ config(materialized='view') }}

WITH title_value AS (
    SELECT
        item_id,
        metadata_value_id,
        text_value_raw AS value_raw,
        NULLIF({{ clean_text("REPLACE(text_value_raw, '|', ' ')") }}, '') AS value
    FROM {{ ref('int_unlp_sedicidb_item_metadatavalue') }}
    WHERE metadatafield_fullname = 'dc.title'
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY item_id
            ORDER BY metadata_value_id DESC
        ) AS value_rank
    FROM title_value
    WHERE value IS NOT NULL
)

SELECT
    item_id,
    metadata_value_id,
    value_raw,
    value
FROM ranked
WHERE value_rank = 1
