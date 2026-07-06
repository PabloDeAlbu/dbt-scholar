WITH source AS (
  SELECT * FROM {{ source('dspacedb5', 'collection2item') }}
),
renamed AS (
  SELECT
    id::bigint AS collection_item_id,
    collection_id::integer AS collection_id,
    item_id::integer AS item_id
  FROM source
),
ghost_record AS (
  SELECT
    -1::bigint AS collection_item_id,
    -1::integer AS collection_id,
    -1::integer AS item_id
)

SELECT * FROM renamed
UNION ALL
SELECT * FROM ghost_record
