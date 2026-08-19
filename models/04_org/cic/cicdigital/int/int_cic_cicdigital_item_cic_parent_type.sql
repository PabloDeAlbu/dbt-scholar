{{ config(materialized='view') }}

WITH ranked AS (
    SELECT
        item_id,
        metadata_value_id,
        value_raw,
        value,
        ROW_NUMBER() OVER (PARTITION BY item_id ORDER BY metadata_value_id DESC) AS value_rank
    FROM {{ ref('int_cic_cicdigital_item_metadatavalue') }}
    WHERE metadatafield_fullname = 'cic.parentType'
      AND value IS NOT NULL
)

SELECT item_id, metadata_value_id, value_raw, value
FROM ranked
WHERE value_rank = 1
