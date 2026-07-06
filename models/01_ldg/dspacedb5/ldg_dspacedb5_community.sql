WITH source AS (
  SELECT * FROM {{ source('dspacedb5', 'community') }}
),
renamed AS (
  SELECT
    community_id::bigint AS community_id,
    logo_bitstream_id::bigint AS logo_bitstream_id,
    admin::integer AS admin
  FROM source
),
ghost_record AS (
  SELECT
    -1::bigint AS community_id,
    -1::bigint AS logo_bitstream_id,
    -1::integer AS admin
)

SELECT * FROM renamed
UNION ALL
SELECT * FROM ghost_record
