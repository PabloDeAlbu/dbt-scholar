WITH source AS (
  SELECT * FROM {{ source('dspacedb5', 'bundle2bitstream') }}
),
renamed AS (
  SELECT
    id::bigint AS id,
    bundle_id::integer AS bundle_id,
    bitstream_id::integer AS bitstream_id,
    bitstream_order::integer AS bitstream_order
  FROM source
),
ghost_record AS (
  SELECT
    -1::bigint AS id,
    -1::integer AS bundle_id,
    -1::integer AS bitstream_id,
    -1::integer AS bitstream_order
)

SELECT * FROM renamed
UNION ALL
SELECT * FROM ghost_record
