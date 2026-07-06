WITH source AS (
  SELECT * FROM {{ source('dspacedb5', 'item2bundle') }}
),
renamed AS (
  SELECT
    id::bigint AS id,
    item_id::integer AS item_id,
    bundle_id::integer AS bundle_id
  FROM source
),
ghost_record AS (
  SELECT
    -1::bigint AS id,
    -1::integer AS item_id,
    -1::integer AS bundle_id
)

SELECT * FROM renamed
UNION ALL
SELECT * FROM ghost_record
