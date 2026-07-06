WITH source AS (
  SELECT * FROM {{ source('dspacedb5', 'handle') }}
),
renamed AS (
  SELECT
    handle_id::bigint AS handle_id,
    handle::text AS handle,
    resource_type_id::integer AS resource_type_id,
    resource_id::integer AS resource_id
  FROM source
),
ghost_record AS (
  SELECT
    -1::bigint AS handle_id,
    '!UNKNOWN'::text AS handle,
    -1::integer AS resource_type_id,
    -1::integer AS resource_id
)

SELECT * FROM renamed
UNION ALL
SELECT * FROM ghost_record
