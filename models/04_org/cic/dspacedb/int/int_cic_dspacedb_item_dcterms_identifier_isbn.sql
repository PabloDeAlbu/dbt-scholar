{{ config(materialized='view') }}

WITH normalized AS (
    SELECT
        item_uuid,
        metadata_value_id,
        value_raw,
        REGEXP_REPLACE(UPPER(value), '[^0-9X]', '', 'g') AS value
    FROM {{ ref('int_cic_dspacedb_item_metadatavalue') }}
    WHERE metadatafield_fullname = 'dcterms.identifier.isbn'
)

SELECT item_uuid, metadata_value_id, value_raw, value
FROM normalized
WHERE LENGTH(value) IN (10, 13)
