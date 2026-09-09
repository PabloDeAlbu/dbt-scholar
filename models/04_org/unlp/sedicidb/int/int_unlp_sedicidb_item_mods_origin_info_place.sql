{{ config(materialized='view') }}

SELECT
    item_id,
    metadata_value_id,
    text_value_raw AS value_raw,
    text_value AS value,
    authority,
    CASE
        WHEN authority ~* '^https?://voc\.sedici\.unlp\.edu\.ar/node/[0-9]+/?$'
            THEN SUBSTRING(authority FROM '/node/([0-9]+)/?$')::bigint
    END AS voc_institution_node_id,
    confidence,
    place
FROM {{ ref('int_unlp_sedicidb_item_metadatavalue') }}
WHERE metadatafield_fullname = 'mods.originInfo.place'
  AND text_value IS NOT NULL
