{{ config(materialized='view') }}

SELECT item_uuid, metadata_value_id, value_raw, value, authority, confidence, place
FROM {{ ref('int_cic_dspacedb_item_metadatavalue') }}
WHERE metadatafield_fullname = 'dcterms.creator.corporate'
  AND value IS NOT NULL
