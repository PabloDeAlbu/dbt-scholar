{{ config(materialized='view') }}

WITH ranked AS (
    SELECT item_uuid, metadata_value_id, value_raw, value,
        ROW_NUMBER() OVER (
            PARTITION BY item_uuid
            ORDER BY metadata_value_id DESC, metadatavalue_hk
        ) AS value_rank
    FROM {{ ref('int_cic_dspacedb_item_metadatavalue') }}
    WHERE metadatafield_fullname = 'cic.parentType'
      AND value IS NOT NULL
)

SELECT item_uuid, metadata_value_id, value_raw, value
FROM ranked
WHERE value_rank = 1
