SELECT
    item_id,
    metadatafield_fullname,
    values
FROM {{ ref('int_unlp_sedicidb_item_metadatafield') }}
WHERE metadatafield_fullname IN (
    'dc.title',
    'dc.type',
    'sedici.subtype'
)
  AND has_conflicting_values
