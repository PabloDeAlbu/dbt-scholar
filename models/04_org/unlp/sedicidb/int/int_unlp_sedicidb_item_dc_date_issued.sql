{{ config(materialized='view') }}

WITH date_value AS (
    SELECT
        item_id,
        metadata_value_id,
        text_value_raw AS value_raw,
        CASE
            WHEN text_value ~ '^[0-9]{4}[-/](0[1-9]|1[0-2])[-/](0[1-9]|[12][0-9]|3[01])'
                THEN TO_DATE(REPLACE(LEFT(text_value, 10), '/', '-'), 'YYYY-MM-DD')
            WHEN text_value ~ '^[0-9]{4}[-/](0[1-9]|1[0-2])$'
                THEN TO_DATE(REPLACE(text_value, '/', '-') || '-01', 'YYYY-MM-DD')
            WHEN text_value ~ '^[0-9]{4}$'
                THEN TO_DATE(text_value || '-01-01', 'YYYY-MM-DD')
        END AS value,
        CASE
            WHEN text_value ~ '^[0-9]{4}[-/](0[1-9]|1[0-2])[-/](0[1-9]|[12][0-9]|3[01])' THEN 'day'
            WHEN text_value ~ '^[0-9]{4}[-/](0[1-9]|1[0-2])$' THEN 'month'
            WHEN text_value ~ '^[0-9]{4}$' THEN 'year'
        END AS value_precision
    FROM {{ ref('int_unlp_sedicidb_item_metadatavalue') }}
    WHERE metadatafield_fullname = 'dc.date.issued'
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY item_id
            ORDER BY metadata_value_id DESC
        ) AS value_rank
    FROM date_value
    WHERE value IS NOT NULL
)

SELECT
    item_id,
    metadata_value_id,
    value_raw,
    value,
    value_precision
FROM ranked
WHERE value_rank = 1
