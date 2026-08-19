{{ config(materialized='view') }}

WITH extracted AS (
    SELECT
        item_id,
        metadata_value_id,
        value_raw,
        REGEXP_REPLACE(
            SUBSTRING(LOWER(value) FROM '(10[.][0-9]{4,9}/[^[:space:]<>|";]+)'),
            '[.),;:]+$',
            ''
        ) AS value
    FROM {{ ref('int_cic_cicdigital_item_metadatavalue') }}
    WHERE metadatafield_fullname = 'dcterms.identifier.other'
)

SELECT item_id, metadata_value_id, value_raw, value
FROM extracted
WHERE value IS NOT NULL
